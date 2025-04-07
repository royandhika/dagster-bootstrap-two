import dagster as dg
from dagster_dbt import dbt_assets, DbtCliResource
from shared.resources import path_dbt
from shared.utils.custom_translator import CustomDbtTranslator
from partitions import partition_daily
import json


@dbt_assets(
    dagster_dbt_translator=CustomDbtTranslator(),
    manifest=path_dbt.manifest_path,
    partitions_def=partition_daily
)
def report_prospect(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    start, end = context.partition_time_window
    dbt_vars = {"min_date": start.isoformat(), "max_date": end.isoformat()}
    run_arg = ["build", "--vars", json.dumps(dbt_vars)]
    yield from dbt.cli(run_arg, context=context).stream().fetch_row_counts().fetch_column_metadata()