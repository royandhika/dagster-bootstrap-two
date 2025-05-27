from dagster import (
    PartitionsDefinition, 
    AssetsDefinition, 
    AssetExecutionContext, 
    AssetSpec, 
    AssetKey, 
    asset, 
    sensor,
    EnvVar, 
    MaterializeResult, 
    MetadataValue, 
    MultiPartitionsDefinition, 
    TimeWindowPartitionsDefinition,
    MultiPartitionKey,
    run_status_sensor,
    DagsterRunStatus,
    JobDefinition,
    RunRequest,
    SkipReason,
    RunsFilter,
    SensorEvaluationContext,
    RunStatusSensorContext,
)
from dagster._core.storage.tags import PARTITION_NAME_TAG
from dagster_dbt import dbt_assets, DbtCliResource
from dagster_sling import sling_assets, SlingResource
from shared.utils.custom_translator import CustomDbtTranslator, CustomDbtRun, CustomSlingTranslator, CustomPandasRun
from shared.utils.custom_function import sling_yaml_dict, sling_add_backfill
from shared.resources import path_dbt, PSSResource
from datetime import timedelta, datetime
import json
import itertools


def make_dbt_asset_with_partition(name: str, select: str, partitions_def: PartitionsDefinition) -> AssetsDefinition:
    @dbt_assets(
        name=name,
        dagster_dbt_translator=CustomDbtTranslator(),
        manifest=path_dbt.manifest_path,
        partitions_def=partitions_def,
        pool="dbt",
        select=select,
    )
    def _dbt_asset(context: AssetExecutionContext, dbt: DbtCliResource, config: CustomDbtRun):
        start, end = context.partition_time_window
        start -= timedelta(minutes=30)
        dbt_vars = {
            "min_date": start.strftime('%Y-%m-%d %H:%M:%S'), 
            "max_date": end.strftime('%Y-%m-%d %H:%M:%S')
        }
        run_arg = ["build", "--vars", json.dumps(dbt_vars)]
        run_arg += ["--threads", f"{config.threads}"]
        if config.full_refresh:
            run_arg += ["--full-refresh"]
        yield from dbt.cli(run_arg, context=context).stream().fetch_row_counts().fetch_column_metadata()

    return _dbt_asset


def make_external_asset(kind: set[str], group:str, tables: list) -> list[AssetSpec]:
    return [
        AssetSpec(
            key=AssetKey(["sources", table]),
            group_name=group,
            kinds=kind
        )
        for table in tables
    ]


def make_sling_asset_with_partition(name: str, sling_file: str, partitions_def: PartitionsDefinition) -> AssetsDefinition:
    @sling_assets(
        name=name,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=sling_yaml_dict(sling_file),
        partitions_def=partitions_def,
        pool="sling",
    )
    def _sling_asset(context: AssetExecutionContext, sling: SlingResource):
        sling_path = sling_yaml_dict(sling_file)
        fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

        yield from sling.replicate(
            context=context,
            dagster_sling_translator=CustomSlingTranslator(),
            replication_config=fixed_yaml,
        )
    return _sling_asset


def make_dbt_sensor_with_partition(name: str, monitored_jobs: list[JobDefinition], request_jobs: list[JobDefinition], interval: int):
    @run_status_sensor(
        name=name,
        run_status=DagsterRunStatus.SUCCESS,
        monitored_jobs=monitored_jobs,
        request_jobs=request_jobs,
        minimum_interval_seconds=60,
    )
    def _sensor(context: RunStatusSensorContext):
        # Custom multiple run per day
        if interval == 24:
            should_run = datetime.now().hour == 0 
        else: 
            should_run = datetime.now().hour % interval == 0
                    
        # Exclude backfill runs
        if context.dagster_run.tags.get("dagster/backfill"):
            yield SkipReason("Skipping backfill run")
        

        if should_run:
            assert request_jobs[0].partitions_def is not None
            partition_keys = request_jobs[0].partitions_def.get_partition_keys() 
            last_partition = partition_keys[-1]
            yield RunRequest(partition_key=last_partition)
        else:
            yield SkipReason(f"Current hour is {datetime.now().strftime('%H')}")
    
    return _sensor


def make_pss_asset_with_partition(name: str, group: str, partitions_def: PartitionsDefinition | MultiPartitionsDefinition, query: str, so_param: bool = False) -> AssetsDefinition:
    @asset(
        name=name,
        partitions_def=partitions_def,
        pool="pss",
        key_prefix=["landings"],
        group_name=group,
        kinds={"sqlserver", "python"},
        metadata={
            "dagster/table_name": f"AWODB.{EnvVar('ENV_SCHEMA').get_value()}_dl.{name}",
        },
    )
    def _pss_asset(context: AssetExecutionContext, pss: PSSResource, config: CustomPandasRun):
        start, end = context.partition_time_window
        start -= timedelta(minutes=30)
        start = start.strftime('%Y-%m-%d %H:%M:%S')
        end = end.strftime('%Y-%m-%d %H:%M:%S')
        table = query

        if config.full_refresh:
            start = "2000-01-01 00:00:00"
            end = "2500-01-01 00:00:00"
            table += "OR COALESCE(LastModifiedTime, CreatedTime) IS NULL" 

        param = (start, end)

        if hasattr(context.partition_key, "keys_by_dimension"):
            so = context.partition_key.keys_by_dimension["so"]  # type: ignore
            if so_param:
                param = (so, start, end)
            table = table.format(so=so)
        
        count = 0
        
        data = pss.read_table(
            query=table,
            params=param,
            table_name=name,
            pandas_method=config.method,
        )

        count += len(data)

        yield MaterializeResult(
            metadata={
                "dagster/row_count": MetadataValue.int(count)
            }
        )
    return _pss_asset


def make_dbt_sensor_with_multiple_required(name: str, monitored_jobs: list[JobDefinition], request_job: JobDefinition, lookback_range: int):
    @sensor(
        name=name,
        job=request_job,
        minimum_interval_seconds=120,
    )
    def _sensor(context: SensorEvaluationContext):
        # Define time window
        time_window = datetime.now() - timedelta(hours=lookback_range)
        all_completed = True
        completion_times = {}
        
        for job in monitored_jobs:
            list_partitions = []
            pdef = job.partitions_def
            # If multi partition: Append 'YYYY-MM-DD|XX' to the list
            if isinstance(pdef, MultiPartitionsDefinition):
                dims = pdef.partitions_defs

                time_dim_def = None
                time_dim_name = None
                
                # Take the time partition first
                for dim in dims:
                    if isinstance(dim.partitions_def, TimeWindowPartitionsDefinition):
                        time_dim_def = dim.partitions_def
                        time_dim_name = dim.name
                        break

                # Take the rest partition (assumably a static partition)
                if time_dim_def:
                    last_time_dim = time_dim_def.get_last_partition_key()

                    other_dim = {
                        dim.name: dim.partitions_def.get_partition_keys()
                        for dim in dims
                        if dim.name != time_dim_name 
                    }

                    if other_dim:
                        other_names = list(other_dim.keys())
                        other_values = list(other_dim.values())

                        for combination in itertools.product(*other_values):
                            partition_dict = {time_dim_name: last_time_dim}
                            for i, other_dim_name in enumerate(other_names):
                                partition_dict[other_dim_name] = combination[i]
                            
                            list_partitions.append(MultiPartitionKey(partition_dict)) # type: ignore
                            
                    else:
                        list_partitions.append(MultiPartitionKey({time_dim_name: last_time_dim})) # type: ignore
                    
            # If single partition: Append 'YYYY-MM-DD' to the list
            elif isinstance(pdef, TimeWindowPartitionsDefinition):
                list_partitions.append(pdef.get_last_partition_key())
            
            # list_partitions = list(set(list_partitions))
            
            # Check every tags matched the list
            for partition in list_partitions:
                run_records = context.instance.get_run_records(
                    filters=RunsFilter(
                        job_name=job.name,
                        statuses=[DagsterRunStatus.SUCCESS],
                        tags={PARTITION_NAME_TAG: partition},
                        created_after=time_window,
                    ),
                    order_by="update_timestamp",
                    ascending=False,
                    limit=1
                )
                # Mark the run status as False when anything fail
                if not run_records:
                    all_completed = False
                    break
                else:
                    completion_times[job.name] = run_records[0].end_time
        
        if all_completed:
            # Generate unique run key based on completion times
            run_key = f"run_{'_'.join(str(t) for t in completion_times.values())}"
            
            # Check if we've already triggered for this combination
            cursor = context.cursor or ""
            if run_key != cursor:
                context.update_cursor(run_key)
                
                # Get the last available partition from requested_job if it has a timewindow partition
                partition_key = None
                if hasattr(request_job, 'partitions_def') and request_job.partitions_def:
                    partition_keys = request_job.partitions_def.get_partition_keys(
                        dynamic_partitions_store=context.instance
                    )
                    if partition_keys:
                        partition_key = partition_keys[-1] 
                
                yield RunRequest(
                    run_key=run_key,
                    partition_key=partition_key
                )
        
        yield SkipReason("Not all monitored jobs completed yet")
    
    return _sensor


# def generic_multi_sensor(name, parents, target_job, lookback_hrs=36):

#     @sensor(name=name, job=target_job)
#     def _sensor(context):
#         # 1) Tanggal yang mau dicek (kemarin)
#         date_key = (datetime.now().astimezone()
#                     - timedelta(days=1)).strftime("%Y-%m-%d")
#         since_ts = datetime.now() - timedelta(hours=lookback_hrs)

#         for job in parents:
#             pdef = job.partitions_def   # Multi- or single-dim
#             if not pdef:
#                 return SkipReason(f"{job.name} tidak ber-partition")

#             # ---- SINGLE DIM (A, B) ---------------------------------
#             if not hasattr(pdef, "dimension_definitions"):
#                 recs = context.instance.get_run_records(
#                     RunsFilter(
#                         job_name=job_name,
#                         statuses=[DagsterRunStatus.SUCCESS],
#                         tags=tag_dict,
#                         created_after=since_ts,
#                     ),
#                     limit=1,
#                 )
#                 return bool(recs)

#                 if not _latest_success(job.name, {PARTITION_NAME_TAG: date_key}, since_ts, context):
#                     return SkipReason(f"{job.name} belum selesai")
#                 continue

#             # ---- MULTI DIM (C) -------------------------------------
#             dims = pdef.dimension_definitions
#             # deteksi dimensi waktu otomatis
#             time_dim = next(d for d in dims
#                             if isinstance(d.partitions_def, TimeWindowPartitionsDefinition))
#             static_dims = [d for d in dims if d != time_dim]

#             # list semua kombinasi static-key utk date_key
#             static_key_lists = [
#                 d.partitions_def.get_partition_keys() for d in static_dims
#             ]
#             for combo in product(*static_key_lists):
#                 mpk = MultiPartitionKey(
#                     {time_dim.name: date_key, **dict(zip([d.name for d in static_dims], combo))}
#                 )
#                 tags = {f"dagster/partition/{k}": v for k, v in mpk.keys_by_dimension().items()}
#                 if not _latest_success(job.name, tags, since_ts, context):
#                     return SkipReason(f"{job.name} belum lengkap utk {date_key}")

#         # ---- Semua parent lengkap ---------------------------------
#         if (context.cursor or "") != date_key:
#             context.update_cursor(date_key)
#             return RunRequest(run_key=date_key, partition_key=date_key)

#         return SkipReason("Sudah pernah dijalankan")

#     return _sensor


# def get_multipartition_keys_with_dimension_value(
#     partition_def: dg.MultiPartitionsDefinition, 
#     dimension_values: dict
# ) -> list:
#     matching_keys = []
#     for partition_key in partition_def.get_partition_keys():
#         keys_by_dimension = partition_key.keys_by_dimension()
#         if all([
#             keys_by_dimension.get(dimension, None) == value
#             for dimension, value in dimension_values.items()
#         ]):
#             matching_keys.append(partition_key)
#     return matching_keys

# @dg.schedule(
#     cron_schedule="0 1 * * *",
#     job=my_multi_partitioned_job,
# )
# def daily_schedule(context):
#     # Mendapatkan tanggal untuk hari sebelumnya
#     previous_day = context.scheduled_execution_time.date() - datetime.timedelta(days=1)
#     date_str = previous_day.strftime("%Y-%m-%d")
    
#     # Mendapatkan semua partition keys untuk tanggal tersebut
#     partition_keys = get_multipartition_keys_with_dimension_value(
#         multi_partitions, 
#         {"date": date_str}
#     )
    
#     # Membuat run request untuk setiap partition
#     for partition_key in partition_keys:
#         yield dg.RunRequest(
#             run_key=partition_key,
#             partition_key=dg.MultiPartitionKey.from_str(partition_key)
#         )


# import dagster as dg

# # Example multi-dimensional partition definition
# daily_partitions = dg.DailyPartitionsDefinition(start_date="2024-01-01")
# region_partitions = dg.StaticPartitionsDefinition(["us", "eu", "jp"])

# multi_partitions = dg.MultiPartitionsDefinition({
#     "date": daily_partitions,
#     "region": region_partitions
# })

# # To get the last available date partition
# def get_last_date_partition(multi_partition_def, current_time=None):
#     # Get all partition keys
#     all_keys = multi_partition_def.get_partition_keys(current_time=current_time)
    
#     # Extract unique dates from the multi-partition keys
#     dates = set()
#     for key in all_keys:
#         keys_by_dimension = key.keys_by_dimension
#         dates.add(keys_by_dimension["date"])
    
#     # Return the latest date
#     return max(dates) if dates else None

# # Usage in a schedule or asset
# @dg.schedule(
#     cron_schedule="0 1 * * *",
#     job=your_multi_partitioned_job
# )
# def multi_partition_schedule(context):
#     # Get the last available date partition
#     last_date = get_last_date_partition(multi_partitions, context.scheduled_execution_time)
    
#     # Create run requests for all regions for the last date
#     run_requests = []
#     for region in region_partitions.get_partition_keys():
#         partition_key = dg.MultiPartitionKey({
#             "date": last_date,
#             "region": region
#         })
#         run_requests.append(dg.RunRequest(
#             partition_key=partition_key,
#             run_key=f"{last_date}|{region}"
#         ))
    
#     return run_requests



# import dagster as dg

# # Asumsi kamu punya job_definition
# partitions_def = job_definition.partitions_def


# # Mendapatkan semua partition keys
# partition_keys = partitions_def.get_partition_keys()

# # Mendapatkan last partition key
# last_partition_key = partition_keys[-1]

# # Atau menggunakan method khusus untuk time partitions
# if hasattr(partitions_def, 'get_last_partition_key'):
#     last_partition_key = partitions_def.get_last_partition_key()


# if isinstance(partitions_def, dg.MultiPartitionsDefinition):
#     # Mendapatkan dimension definitions
#     dimensions = partitions_def.partitions_defs
    
#     # Asumsi daily partition ada di dimension 'date'
#     daily_dimension = dimensions.get('date')  # atau 'daily'
    
#     if daily_dimension:
#         # Mendapatkan last daily partition
#         last_daily_partition = daily_dimension.get_last_partition_key()
        
#         # Untuk mendapatkan semua partition keys dengan last daily
#         static_dimension = dimensions.get('region')  # atau nama dimension static lainnya
#         if static_dimension:
#             static_keys = static_dimension.get_partition_keys()
            
#             # Membuat MultiPartitionKey untuk setiap kombinasi
#             last_daily_partitions = [
#                 dg.MultiPartitionKey({
#                     'date': last_daily_partition,
#                     'region': static_key  # atau nama dimension yang sesuai
#                 })
#                 for static_key in static_keys
#             ]
