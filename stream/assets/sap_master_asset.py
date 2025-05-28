from shared.partitions import partition_daily 
from shared.utils.custom_asset import make_sap_asset_with_partition
import sap
    

masters = {
    "group": "sap_awo_master",
    "partitions_def": partition_daily,
    "lead": 1,
    "tables": [
        {
            "name": "awo_detail_dealer_sap_staging",
            "file": sap.AWO_DETAIL_DEALER
        },
    ]
}

for table in masters["tables"]:
    globals()[table["name"]] = make_sap_asset_with_partition(
        name=table["name"],
        group=masters["group"],
        partitions_def=masters["partitions_def"],
        file_name=table["file"],
        lead=masters["lead"],
    )