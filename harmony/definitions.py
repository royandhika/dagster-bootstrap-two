from dagster import Definitions, load_assets_from_modules
from shared.resources import resource_sling, resource_dbt, resource_api
from assets import source_asset, sling_ecentrix_asset, sling_inbound_asset, sling_outbound_asset, api_asset, dbt_ecentrix_asset, dbt_inbound_asset, dbt_outbound_asset
from jobs import jobs, schedules
from sensors import sensors


asset_dbt = load_assets_from_modules(modules=[dbt_ecentrix_asset, dbt_inbound_asset, dbt_outbound_asset])
asset_sling = load_assets_from_modules(modules=[sling_ecentrix_asset, sling_inbound_asset, sling_outbound_asset])
asset_api = load_assets_from_modules(modules=[api_asset])

defs = Definitions(
    resources={
        "dbt": resource_dbt,
        "sling": resource_sling,
        "api_crm": resource_api,
    },
    assets=list(asset_dbt) + list(asset_sling) + source_asset.external + list(asset_api),
    jobs=jobs,
    schedules=schedules,
    sensors=sensors,
)
