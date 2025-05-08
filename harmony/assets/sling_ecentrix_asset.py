from shared.utils.custom_asset import make_sling_asset_with_partition
from shared.partitions import partition_hourly


slings = [
    {
        "name": "sling_ecentrix_telephony",
        "sling_file": "ecentrix_telephony.yaml",
        "partitions_def": partition_hourly
    }
]

for sling in slings:
    globals()[sling["name"]] = make_sling_asset_with_partition(
        name=sling["name"],
        sling_file=sling["sling_file"],
        partitions_def=sling["partitions_def"]
    )