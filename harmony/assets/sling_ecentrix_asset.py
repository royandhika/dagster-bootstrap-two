from shared.utils.custom_asset import make_sling_asset_with_partition
from shared.partitions import partition_hourly


slings = [
    {
        "name": "sling_ecentrix_alpha",
        "partitions_def": partition_hourly,
        "files": [
            "ecentrix_alpha/ecentrix_session_log.yaml",
            "ecentrix_alpha/ecentrix_reference.yaml",
        ],
    },
    {
        "name": "sling_ecentrix_bravo",
        "partitions_def": partition_hourly,
        "files": [
            "ecentrix_bravo/ecentrix_session_log.yaml",
            "ecentrix_bravo/ecentrix_reference.yaml",
        ],
    },
    {
        "name": "sling_ecentrix_predictive",
        "partitions_def": partition_hourly,
        "files": [
            "ecentrix_predictive/ecentrix_session_log.yaml",
            "ecentrix_predictive/ecentrix_reference.yaml",
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