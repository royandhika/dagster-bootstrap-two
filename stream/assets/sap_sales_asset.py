from shared.partitions import partition_daily 
from shared.utils.custom_asset import make_sap_asset_with_partition
import sap
    

sales = {
    "group": "sap_awo_sales",
    "partitions_def": partition_daily,
    "lead": 1,
    "tables": [
        {
            "name": "awo_transaction_sales_sap_staging",
            "file": sap.AWO_TRANSACTION_SALES
        },
        {
            "name": "awo_transaction_sales_v2_sap_staging",
            "file": sap.AWO_TRANSACTION_SALES_V2
        },
        {
            "name": "awo_transaction_sales_invoice_sap_staging",
            "file": sap.AWO_TRANSACTION_SALES_INVOICE
        },
        {
            "name": "awo_transaction_sales_partnerdetail_sap_staging",
            "file": sap.AWO_TRANSACTION_SALES_PARTNERDETAIL
        },
        {
            "name": "awo_transaction_sales_salesorder_sap_staging",
            "file": sap.AWO_TRANSACTION_SALES_SALESORDER
        },
    ]
}

for table in sales["tables"]:
    globals()[table["name"]] = make_sap_asset_with_partition(
        name=table["name"],
        group=sales["group"],
        partitions_def=sales["partitions_def"],
        file_name=table["file"],
        lead=sales["lead"],
    )