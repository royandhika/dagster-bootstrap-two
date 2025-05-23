from shared.utils.custom_asset import make_dbt_asset_with_partition
from shared.partitions import partition_daily


pss_pkb = [
    {
        "name": "dbt_awo_transaction_pkb_pss",
        "select": "awo_transaction_pkb_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_pkb_billing_alt_pss",
        "select": "awo_transaction_pkb_billing_alt_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_pkb_billing_pss",
        "select": "awo_transaction_pkb_billing_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_pkb_booking_pss",
        "select": "awo_transaction_pkb_booking_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_pkb_material_pss",
        "select": "awo_transaction_pkb_material_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_pkb_operation_pss",
        "select": "awo_transaction_pkb_operation_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_pkb_orderrequest_pss",
        "select": "awo_transaction_pkb_orderrequest_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_pkb_partnerdetail_pss",
        "select": "awo_transaction_pkb_partnerdetail_pss",
        "partitions_def": partition_daily
    },
    {
        "name": "dbt_awo_transaction_pkb_salesorder_pss",
        "select": "awo_transaction_pkb_salesorder_pss",
        "partitions_def": partition_daily
    },
]

for asset in pss_pkb:
    globals()[asset["name"]] = make_dbt_asset_with_partition(
        name=asset["name"],
        select=asset["select"],
        partitions_def=asset["partitions_def"]
    )