from shared.utils.custom_asset import make_sling_asset_with_partition
from shared.partitions import partition_2hourly


slings = [
    {
        "name": "sling_inbound_awda",
        "sling_file": "inbound_awda.yaml",
        "partitions_def": partition_2hourly
    },
    {
        "name": "sling_inbound_awo",
        "sling_file": "inbound_awo.yaml",
        "partitions_def": partition_2hourly
    },
    {
        "name": "sling_inbound_nasmoco",
        "sling_file": "inbound_nasmoco.yaml",
        "partitions_def": partition_2hourly
    },
    {
        "name": "sling_inbound_omni_astralife",
        "sling_file": "inbound_omni_astralife.yaml",
        "partitions_def": partition_2hourly
    },
    {
        "name": "sling_inbound_shopanddrive_v4",
        "sling_file": "inbound_shopanddrive_v4.yaml",
        "partitions_def": partition_2hourly
    },
    {
        "name": "sling_inbound_taf",
        "sling_file": "inbound_taf.yaml",
        "partitions_def": partition_2hourly
    },
]

for sling in slings:
    globals()[sling["name"]] = make_sling_asset_with_partition(
        name=sling["name"],
        sling_file=sling["sling_file"],
        partitions_def=sling["partitions_def"]
    )