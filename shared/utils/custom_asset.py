from dagster import PartitionsDefinition, AssetsDefinition, AssetExecutionContext, AssetSpec, AssetKey
from dagster_dbt import dbt_assets, DbtCliResource
from dagster_sling import sling_assets, SlingResource
from shared.utils.custom_translator import CustomDbtTranslator, CustomDbtRun, CustomSlingTranslator
from shared.utils.custom_function import sling_yaml_dict, sling_add_backfill
from shared.resources import path_dbt
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
            # description=table["desc"],
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