from shared.utils.custom_asset import make_dbt_asset_with_partition
from shared.partitions import partition_daily


pss_customer = [
    {
        "name": "dbt_awo_customer_pss",
        "select": "awo_customer_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_customer_address_pss",
        "select": "awo_customer_address_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_customer_addressusage_pss",
        "select": "awo_customer_addressusage_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_customer_email_pss",
        "select": "awo_customer_email_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_customer_fax_pss",
        "select": "awo_customer_fax_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_customer_flat_pss",
        "select": "awo_customer_flat_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_customer_marketingattribute_pss",
        "select": "awo_customer_marketingattribute_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_customer_telephone_pss",
        "select": "awo_customer_telephone_pss",
        "partitions_def": partition_daily
    },
]

for asset in pss_customer:
    globals()[asset["name"]] = make_dbt_asset_with_partition(
        name=asset["name"],
        select=asset["select"],
        partitions_def=asset["partitions_def"]
    )