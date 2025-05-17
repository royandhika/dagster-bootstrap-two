from shared.utils.custom_asset import make_sling_asset_with_partition
from shared.partitions import partition_2hourly


slings = [
    {
        "name": "sling_inbound_awda",
        "partitions_def": partition_2hourly,
        "files": [
            "inbound_awda/cc_master_category.yaml",
            "inbound_awda/cc_master_customer.yaml",
            "inbound_awda/cc_master_reference.yaml",
            "inbound_awda/cc_queue_detail.yaml",
            "inbound_awda/cc_queue.yaml",
        ],
    },
    {
        "name": "sling_inbound_awo",
        "partitions_def": partition_2hourly,
        "files": [
            "inbound_awo/cc_master_category.yaml",
            "inbound_awo/cc_master_customer.yaml",
            "inbound_awo/cc_master_reference.yaml",
            "inbound_awo/cc_queue_detail.yaml",
            "inbound_awo/cc_queue.yaml",
        ],
    },
    {
        "name": "sling_inbound_nasmoco",
        "partitions_def": partition_2hourly,
        "files": [
            "inbound_nasmoco/cc_master_category.yaml",
            "inbound_nasmoco/cc_master_customer.yaml",
            "inbound_nasmoco/cc_master_reference.yaml",
            "inbound_nasmoco/cc_queue_detail.yaml",
            "inbound_nasmoco/cc_queue.yaml",
        ],
    },
    {
        "name": "sling_inbound_omni_astralife",
        "partitions_def": partition_2hourly,
        "files": [
            "inbound_omni_astralife/cc_queue.yaml",
            "inbound_omni_astralife/wa_inbox.yaml",
        ],
    },
    {
        "name": "sling_inbound_shopanddrive_v4",
        "partitions_def": partition_2hourly,
        "files": [
            "inbound_shopanddrive_v4/cc_master_category.yaml",
            "inbound_shopanddrive_v4/cc_master_customer.yaml",
            "inbound_shopanddrive_v4/cc_master_reference.yaml",
            "inbound_shopanddrive_v4/cc_queue_detail.yaml",
            "inbound_shopanddrive_v4/cc_queue.yaml",
        ],
    },
    {
        "name": "sling_inbound_taf",
        "partitions_def": partition_2hourly,
        "files": [
            "inbound_taf/cc_master_category.yaml",
            "inbound_taf/cc_master_customer.yaml",
            "inbound_taf/cc_master_reference.yaml",
            "inbound_taf/cc_queue_detail.yaml",
            "inbound_taf/cc_queue.yaml",
            "inbound_taf/wa_inbox.yaml",
        ],
    },
]

for sling in slings:
    for file in sling["files"]:
        ops_name = sling["name"] + "_" + file.split('/')[-1].split('.')[0]
        globals()[ops_name] = make_sling_asset_with_partition(
            name=ops_name,
            sling_file=file,
            partitions_def=sling["partitions_def"]
        )