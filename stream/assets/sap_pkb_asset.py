from shared.partitions import partition_daily 
from shared.utils.custom_asset import make_sap_asset_with_partition
import sap
    

pkbs = {
    "group": "sap_awo_pkb",
    "partitions_def": partition_daily,
    "tables": [
        {
            "name": "awo_transaction_pkb_sap_staging",
            "file": sap.AWO_TRANSACTION_PKB
        },
        {
            "name": "awo_transaction_pkb_billing_sap_staging",
            "file": sap.AWO_TRANSACTION_PKB_BILLING
        },
        {
            "name": "awo_transaction_pkb_booking_sap_staging",
            "file": sap.AWO_TRANSACTION_PKB_BOOKING
        },
        {
            "name": "awo_transaction_pkb_material_sap_staging",
            "file": sap.AWO_TRANSACTION_PKB_MATERIAL
        },
        {
            "name": "awo_transaction_pkb_operation_sap_staging",
            "file": sap.AWO_TRANSACTION_PKB_OPERATION
        },
        {
            "name": "awo_transaction_pkb_orderrequest_sap_staging",
            "file": sap.AWO_TRANSACTION_PKB_ORDERREQUEST
        },
        {
            "name": "awo_transaction_pkb_partnerdetail_sap_staging",
            "file": sap.AWO_TRANSACTION_PKB_PARTNERDETAIL
        },
        {
            "name": "awo_transaction_pkb_salesorder_sap_staging",
            "file": sap.AWO_TRANSACTION_PKB_SALESORDER
        },
    ]
}

for table in pkbs["tables"]:
    globals()[table["name"]] = make_sap_asset_with_partition(
        name=table["name"],
        group=pkbs["group"],
        partitions_def=pkbs["partitions_def"],
        file_name=table["file"],
    )