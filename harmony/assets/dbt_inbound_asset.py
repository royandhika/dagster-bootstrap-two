from shared.utils.custom_asset import make_dbt_asset_with_partition
from shared.partitions import partition_daily


dbt_inbound = [
    {
        "name": "dbt_inbound_awda",
        "select": "group:inbound_awda",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_inbound_awo",
        "select": "group:inbound_awo",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_inbound_nasmoco",
        "select": "group:inbound_nasmoco",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_inbound_taf",
        "select": "group:inbound_taf",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_inbound_trac",
        "select": "group:inbound_trac",
        "partitions_def": partition_daily
    },
]

for dbt in dbt_inbound:
    globals()[dbt["name"]] = make_dbt_asset_with_partition(
        name=dbt["name"],
        select=dbt["select"],
        partitions_def=dbt["partitions_def"]
    )