from shared.utils.custom_asset import make_dbt_asset_with_partition
from shared.partitions import partition_3hourly, partition_12hourly


dbt_outbound = [
    {
        "name": "dbt_outbound_adm",
        "select": "group:outbound_adm",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_outbound_ahm",
        "select": "group:outbound_ahm",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_outbound_clipan_duitcair",
        "select": "group:outbound_clipan_duitcair",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_outbound_deskcollfif",
        "select": "group:outbound_deskcollfif",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_outbound_deskcolltaf",
        "select": "group:outbound_deskcolltaf",
        "partitions_def": partition_3hourly
    },
    {
        "name": "dbt_outbound_esvi",
        "select": "group:outbound_esvi",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_outbound_jmfi_mycash",
        "select": "group:outbound_jmfi_mycash",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_outbound_mrs_iso",
        "select": "group:outbound_mrs_iso",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_outbound_mrsdso",
        "select": "group:outbound_mrsdso",
        "partitions_def": partition_12hourly
    },
    {
        "name": "dbt_outbound_tafteleacquisition",
        "select": "group:outbound_tafteleacquisition",
        "partitions_def": partition_3hourly
    },
    {
        "name": "dbt_outbound_tam_concierge",
        "select": "group:outbound_tam_concierge",
        "partitions_def": partition_12hourly
    },
]

for dbt in dbt_outbound:
    globals()[dbt["name"]] = make_dbt_asset_with_partition(
        name=dbt["name"],
        select=dbt["select"],
        partitions_def=dbt["partitions_def"]
    )