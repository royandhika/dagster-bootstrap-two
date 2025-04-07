import dagster as dg 
from dagster_sling import sling_assets, SlingResource
from shared.utils.custom_translator import CustomSlingTranslator
from shared.utils.custom_function import sling_yaml_dict, sling_add_backfill
from partitions import partition_daily

@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("ecentrix_telephony.yaml"),
    partitions_def=partition_daily
)
def ecentrix_telephony(context: dg.AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("ecentrix_telephony.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_key)
    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml
    )

@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("outbound_mrsdso.yaml"),
)
def outbound_mrsdso(context: dg.AssetExecutionContext, sling: SlingResource):
    yield from sling.replicate(context=context)

@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("outbound_aop.yaml"),
)
def outbound_aop(context: dg.AssetExecutionContext, sling: SlingResource):
    yield from sling.replicate(context=context)