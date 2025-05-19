from dagster import (
    PartitionsDefinition, 
    AssetsDefinition, 
    AssetExecutionContext, 
    AssetSpec, 
    AssetKey, 
    BackfillPolicy, 
    asset, 
    EnvVar, 
    MaterializeResult, 
    MetadataValue, 
    MultiPartitionsDefinition, 
    run_status_sensor,
    DagsterRunStatus,
    JobDefinition,
    RunRequest,
    SkipReason
)
from dagster_dbt import dbt_assets, DbtCliResource
from dagster_sling import sling_assets, SlingResource
from shared.utils.custom_translator import CustomDbtTranslator, CustomDbtRun, CustomSlingTranslator, CustomPandasRun
from shared.utils.custom_function import sling_yaml_dict, sling_add_backfill
from shared.resources import path_dbt, PSSResource
from datetime import timedelta, datetime
import json


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


def make_dbt_sensors_with_partition(name: str, monitored_jobs: list[JobDefinition], request_jobs: list[JobDefinition], interval: int):
    @run_status_sensor(
        name=name,
        run_status=DagsterRunStatus.SUCCESS,
        monitored_jobs=monitored_jobs,
        request_jobs=request_jobs,
        minimum_interval_seconds=60,
    )
    def _sensor(context):
        if interval == 24:
            should_run = datetime.now().hour == 0 
        else: 
            should_run = datetime.now().hour % interval == 0
                    
        if should_run:
            assert request_jobs[0].partitions_def is not None
            partition_keys = request_jobs[0].partitions_def.get_partition_keys() 
            last_partition = partition_keys[-1]
            yield RunRequest(partition_key=last_partition)
        else:
            yield SkipReason(f"Current hour is {datetime.now().strftime('%H')}")
    
    return _sensor


def make_pss_asset_with_partition(name: str, group: str, partitions_def: PartitionsDefinition | MultiPartitionsDefinition, query: str) -> AssetsDefinition:
    @asset(
        name=name,
        partitions_def=partitions_def,
        backfill_policy=BackfillPolicy.single_run(),
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