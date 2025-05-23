from shared.utils.custom_asset import make_dbt_asset_with_partition
from shared.partitions import partition_daily


pss_master = [
    {
        "name": "dbt_awo_detail_activitytype_pss",
        "select": "awo_detail_activitytype_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_billingtype_pss",
        "select": "awo_detail_billingtype_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_chargeto_pss",
        "select": "awo_detail_chargeto_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_city_pss",
        "select": "awo_detail_city_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_dealer_pss",
        "select": "awo_detail_dealer_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_employee_pss",
        "select": "awo_detail_employee_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_jobtype_pss",
        "select": "awo_detail_jobtype_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_partnerfunction_pss",
        "select": "awo_detail_partnerfunction_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_pkbtype_pss",
        "select": "awo_detail_pkbtype_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_position_pss",
        "select": "awo_detail_position_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_detail_region_pss",
        "select": "awo_detail_region_pss",
        "partitions_def": partition_daily
    },
]

for asset in pss_master:
    globals()[asset["name"]] = make_dbt_asset_with_partition(
        name=asset["name"],
        select=asset["select"],
        partitions_def=asset["partitions_def"]
    )