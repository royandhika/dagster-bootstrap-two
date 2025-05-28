from shared.partitions.static_so import partition_so_daily, partition_daily, partition_5so_daily, partition_2so_daily
from shared.utils.custom_asset import make_pss_asset_with_partition
import pss.sales as sales


sale = {
    "group": "pss_awo_sales",
    "tables": [
        {
            "name": "awo_transaction_sales_pss_staging",
            "query": sales.AWO_TRANSACTION_SALES,
            "partitions_def": partition_so_daily,
            "so_param": True,
        },
        {
            "name": "awo_transaction_sales_invoice_tso_pss_staging",
            "query": sales.AWO_TRANSACTION_SALES_INVOICE_TSO,
            "partitions_def": partition_daily,
            "so_param": False,
        },
        {
            "name": "awo_transaction_sales_invoice_pss_staging",
            "query": sales.AWO_TRANSACTION_SALES_INVOICE_NONTSO,
            "partitions_def": partition_5so_daily,
            "so_param": False,
        },
        {
            "name": "awo_transaction_sales_partnerdetail_pss_staging",
            "query": sales.AWO_TRANSACTION_SALES_PARTNERDETAIL,
            "partitions_def": partition_daily,
            "so_param": False,
        },
        {
            "name": "awo_transaction_sales_salesorder_pss_staging",
            "query": sales.AWO_TRANSACTION_SALES_SALESORDER,
            "partitions_def": partition_so_daily,
            "so_param": True,
        },
        {
            "name": "awo_transaction_sales_tvc_pss_staging",
            "query": sales.AWO_TRANSACTION_SALES_TVC,
            "partitions_def": partition_2so_daily,
            "so_param": False,
        },
    ]
}

for table in sale["tables"]:
    globals()[table["name"]] = make_pss_asset_with_partition(
        name=table["name"],
        group=sale["group"],
        partitions_def=table["partitions_def"],
        query=table["query"],
        so_param=table["so_param"],
    )