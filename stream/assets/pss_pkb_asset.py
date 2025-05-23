from shared.partitions.static_so import partition_so_daily
from shared.utils.custom_asset import make_pss_asset_with_partition
import pss.pkb as pkb


pkbs = {
    "group": "pss_awo_pkb",
    "partitions_def": partition_so_daily,
    "so_param": True,
    "tables": [
        {
            "name": "awo_transaction_pkb_pss_staging",
            "query": pkb.AWO_TRANSACTION_PKB,
        },
        {
            "name": "awo_transaction_pkb_billing_alt_pss_staging",
            "query": pkb.AWO_TRANSACTION_PKB_BILLING_ALT,
        },
        {
            "name": "awo_transaction_pkb_billing_pss_staging",
            "query": pkb.AWO_TRANSACTION_PKB_BILLING,
        },
        {
            "name": "awo_transaction_pkb_booking_pss_staging",
            "query": pkb.AWO_TRANSACTION_PKB_BOOKING,
        },
        {
            "name": "awo_transaction_pkb_material_pss_staging",
            "query": pkb.AWO_TRANSACTION_PKB_MATERIAL,
        },
        {
            "name": "awo_transaction_pkb_operation_pss_staging",
            "query": pkb.AWO_TRANSACTION_PKB_OPERATION,
        },
        {
            "name": "awo_transaction_pkb_orderrequest_pss_staging",
            "query": pkb.AWO_TRANSACTION_PKB_ORDERREQUEST,
        },
        {
            "name": "awo_transaction_pkb_partnerdetail_pss_staging",
            "query": pkb.AWO_TRANSACTION_PKB_PARTNERDETAIL,
        },
        {
            "name": "awo_transaction_pkb_salesorder_pss_staging",
            "query": pkb.AWO_TRANSACTION_PKB_SALESORDER,
        },
    ]
}

for table in pkbs["tables"]:
    globals()[table["name"]] = make_pss_asset_with_partition(
        name=table["name"],
        group=pkbs["group"],
        partitions_def=pkbs["partitions_def"],
        query=table["query"],
        so_param=pkbs["so_param"],
    )