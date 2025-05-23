from shared.utils.custom_asset import make_dbt_asset_with_partition
from shared.partitions import partition_daily


pss_sales = [
    {
        "name": "dbt_awo_transaction_sales_pss",
        "select": "awo_transaction_sales_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_sales_invoice_pss",
        "select": "awo_transaction_sales_invoice_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_sales_invoice_tso_pss",
        "select": "awo_transaction_sales_invoice_tso_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_sales_partnerdetail_pss",
        "select": "awo_transaction_sales_partnerdetail_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_sales_salesorder_pss",
        "select": "awo_transaction_sales_salesorder_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_sales_tvc_pss",
        "select": "awo_transaction_sales_tvc_pss",
        "partitions_def": partition_daily
    },
]

for asset in pss_sales:
    globals()[asset["name"]] = make_dbt_asset_with_partition(
        name=asset["name"],
        select=asset["select"],
        partitions_def=asset["partitions_def"]
    )