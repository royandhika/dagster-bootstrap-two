from dagster import AssetExecutionContext
from dagster_sling import sling_assets, SlingResource
from shared.utils.custom_translator import CustomSlingTranslator
from shared.utils.custom_function import sling_yaml_dict, sling_add_backfill
from shared.partitions import partition_hourly


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("ecentrix_telephony.yaml"),
    partitions_def=partition_hourly,
    pool="sling",
)
def ecentrix_telephony(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("ecentrix_telephony.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )
