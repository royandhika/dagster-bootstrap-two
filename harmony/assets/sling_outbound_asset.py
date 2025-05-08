from shared.utils.custom_asset import make_sling_asset_with_partition
from shared.partitions import partition_hourly


slings = [
    {
        "name": "sling_outbound_adm",
        "sling_file": "outbound_adm.yaml",
        "partitions_def": partition_hourly
    },
    {
        "name": "sling_outbound_ahm",
        "sling_file": "outbound_ahm.yaml",
        "partitions_def": partition_hourly
    },
    {
        "name": "sling_outbound_esvi",
        "sling_file": "outbound_esvi.yaml",
        "partitions_def": partition_hourly
    },
    {
        "name": "sling_outbound_mrs_iso",
        "sling_file": "outbound_mrs_iso.yaml",
        "partitions_def": partition_hourly
    },
    {
        "name": "sling_outbound_mrsdso",
        "sling_file": "outbound_mrsdso.yaml",
        "partitions_def": partition_hourly
    },
    {
        "name": "sling_outbound_tafteleacquisition",
        "sling_file": "outbound_tafteleacquisition.yaml",
        "partitions_def": partition_hourly
    },
    {
        "name": "sling_outbound_deskcollfif",
        "sling_file": "outbound_deskcollfif.yaml",
        "partitions_def": partition_hourly
    },
]

for sling in slings:
    globals()[sling["name"]] = make_sling_asset_with_partition(
        name=sling["name"],
        sling_file=sling["sling_file"],
        partitions_def=sling["partitions_def"]
    )
