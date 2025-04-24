from dagster import Definitions, load_assets_from_modules
from shared.resources import resource_sling, resource_dbt
from assets import dbt_asset, source_asset, sling_ecentrix_asset, sling_inbound_asset, sling_outbound_asset
from jobs import jobs, schedules
from sensors import sensors


asset_dbt = load_assets_from_modules(modules=[dbt_asset])
asset_sling = load_assets_from_modules(modules=[sling_ecentrix_asset, sling_inbound_asset, sling_outbound_asset])

defs = Definitions(
    resources={
        "dbt": resource_dbt,
        "sling": resource_sling,
    },
    assets=list(asset_dbt) + list(asset_sling) + source_asset.external,
    jobs=jobs,
    schedules=schedules,
    sensors=sensors,
)
