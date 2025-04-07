from dagster import Definitions, load_assets_from_modules
from shared.resources import resource_sling, resource_dbt
from assets import dbt_asset, sling_asset, source_asset, dummy_asset
from jobs import job_outbound_mrsdso


asset_dbt = load_assets_from_modules(modules=[dbt_asset])
asset_sling = load_assets_from_modules(modules=[sling_asset])
asset_dummy = load_assets_from_modules(modules=[dummy_asset])

defs = Definitions(
    resources={
        "dbt": resource_dbt,
        "sling": resource_sling,
    },
    assets=asset_dummy + asset_dbt + asset_sling + source_asset.external,
    jobs=[job_outbound_mrsdso],
    # sensors=
)
