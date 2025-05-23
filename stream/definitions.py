from dagster import Definitions, load_assets_from_modules
from shared.resources import resource_pss, resource_dbt
from assets import (
    pss_customer_asset, 
    pss_master_asset, 
    pss_pkb_asset, 
    pss_sales_asset,
    dbt_pss_customer_asset,
    dbt_pss_master_asset,
    dbt_pss_pkb_asset,
    dbt_pss_sales_asset,
)
from jobs.job_pss import jobs
from schedules import schedules
from sensors import sensors


asset_pss = load_assets_from_modules(modules=[pss_customer_asset, pss_master_asset, pss_pkb_asset, pss_sales_asset])
asset_dbt = load_assets_from_modules(modules=[dbt_pss_customer_asset, dbt_pss_master_asset, dbt_pss_pkb_asset, dbt_pss_sales_asset])

defs = Definitions(
    resources={
        "pss": resource_pss,
        "dbt": resource_dbt
    },
    assets=list(asset_pss) + list(asset_dbt),
    jobs=jobs,
    schedules=schedules,
    sensors=sensors,
)
