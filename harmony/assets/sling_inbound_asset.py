from dagster import AssetExecutionContext
from dagster_sling import sling_assets, SlingResource
from shared.utils.custom_translator import CustomSlingTranslator
from shared.utils.custom_function import sling_yaml_dict, sling_add_backfill
from shared.partitions import partition_2hourly


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("inbound_awda.yaml"),
    partitions_def=partition_2hourly,
    pool="sling",
)
def inbound_awda(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("inbound_awda.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("inbound_awo.yaml"),
    partitions_def=partition_2hourly,
    pool="sling",
)
def inbound_awo(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("inbound_awo.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("inbound_nasmoco.yaml"),
    partitions_def=partition_2hourly,
    pool="sling",
)
def inbound_nasmoco(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("inbound_nasmoco.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("inbound_omni_astralife.yaml"),
    partitions_def=partition_2hourly,
    pool="sling",
)
def inbound_omni_astralife(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("inbound_omni_astralife.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("inbound_shopanddrive_v4.yaml"),
    partitions_def=partition_2hourly,
    pool="sling",
)
def inbound_shopanddrive_v4(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("inbound_shopanddrive_v4.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("inbound_taf.yaml"),
    partitions_def=partition_2hourly,
    pool="sling",
)
def inbound_taf(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("inbound_taf.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )
