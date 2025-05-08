from shared.utils.custom_asset import make_dbt_asset_with_partition
from shared.partitions import partition_12hourly


dbt_ecentrix = [
    {
        "name": "dbt_ecentrix_alpha",
        "select": "group:ecentrix_alpha",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_ecentrix_bravo",
        "select": "group:ecentrix_bravo",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_ecentrix_predictive",
        "select": "group:ecentrix_predictive",
        "partitions_def": partition_12hourly
    },
]

for dbt in dbt_ecentrix:
    globals()[dbt["name"]] = make_dbt_asset_with_partition(
        name=dbt["name"],
        select=dbt["select"],
        partitions_def=dbt["partitions_def"]
    )