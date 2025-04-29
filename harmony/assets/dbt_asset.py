from dagster import AssetExecutionContext
from dagster_dbt import dbt_assets, DbtCliResource
from shared.resources import path_dbt
from shared.utils.custom_translator import CustomDbtTranslator, CustomDbtRun
from shared.partitions import partition_3hourly, partition_12hourly
import json


@dbt_assets(
    dagster_dbt_translator=CustomDbtTranslator(),
    manifest=path_dbt.manifest_path,
    partitions_def=partition_12hourly,
    pool="dbt",
    select="tag:12hourly",
)
def ops_datamart_12hourly(context: AssetExecutionContext, dbt: DbtCliResource, config: CustomDbtRun):
    start, end = context.partition_time_window
    dbt_vars = {
        "min_date": start.isoformat(), 
        "max_date": end.isoformat()
    }
    run_arg = ["build", "--vars", json.dumps(dbt_vars)]
    run_arg += ["--threads", f"{config.threads}"]
    if config.full_refresh:
        run_arg += ["--full-refresh"]
    yield from dbt.cli(run_arg, context=context).stream().fetch_row_counts().fetch_column_metadata()


@dbt_assets(
    dagster_dbt_translator=CustomDbtTranslator(),
    manifest=path_dbt.manifest_path,
    partitions_def=partition_3hourly,
    pool="dbt",
    select="tag:3hourly"
)
def ops_datamart_3hourly(context: AssetExecutionContext, dbt: DbtCliResource, config: CustomDbtRun):
    start, end = context.partition_time_window
    dbt_vars = {
        "min_date": start.isoformat(), 
        "max_date": end.isoformat()
    }
    run_arg = ["build", "--vars", json.dumps(dbt_vars)]
    run_arg += ["--threads", f"{config.threads}"]
    if config.full_refresh:
        run_arg += ["--full-refresh"]
    yield from dbt.cli(run_arg, context=context).stream().fetch_row_counts().fetch_column_metadata()