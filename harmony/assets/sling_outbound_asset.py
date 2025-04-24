from dagster import AssetExecutionContext
from dagster_sling import sling_assets, SlingResource
from shared.utils.custom_translator import CustomSlingTranslator
from shared.utils.custom_function import sling_yaml_dict, sling_add_backfill
from shared.partitions import partition_hourly


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("outbound_adm.yaml"),
    partitions_def=partition_hourly,
    pool="sling",
)
def outbound_adm(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("outbound_adm.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("outbound_ahm.yaml"),
    partitions_def=partition_hourly,
)
def outbound_ahm(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("outbound_ahm.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("outbound_esvi.yaml"),
    partitions_def=partition_hourly,
)
def outbound_esvi(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("outbound_esvi.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("outbound_mrs_iso.yaml"),
    partitions_def=partition_hourly,
)
def outbound_mrs_iso(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("outbound_mrs_iso.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("outbound_mrsdso.yaml"),
    partitions_def=partition_hourly,
)
def outbound_mrsdso(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("outbound_mrsdso.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("outbound_tafteleacquisition.yaml"),
    partitions_def=partition_hourly,
)
def outbound_tafteleacquisition(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("outbound_tafteleacquisition.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )


@sling_assets(
    dagster_sling_translator=CustomSlingTranslator(),
    replication_config=sling_yaml_dict("outbound_deskcollfif.yaml"),
    partitions_def=partition_hourly,
)
def outbound_deskcollfif(context: AssetExecutionContext, sling: SlingResource):
    sling_path = sling_yaml_dict("outbound_deskcollfif.yaml")
    fixed_yaml = sling_add_backfill(sling_path, context.partition_time_window)

    yield from sling.replicate(
        context=context,
        dagster_sling_translator=CustomSlingTranslator(),
        replication_config=fixed_yaml,
    )
