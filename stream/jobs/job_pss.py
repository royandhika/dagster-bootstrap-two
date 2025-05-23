from dagster import AssetSelection, define_asset_job, AssetKey
from shared.partitions import partition_daily
from shared.partitions.static_so import partition_so_daily, partition_4so_daily, partition_2so_daily


jobs = []
dicts = {}

job_configs = [
    # LANDING
    {
        "name": "landing_pss_customer",
        "selection": AssetSelection.groups("pss_awo_customer") & AssetSelection.key_prefixes("landings"),
        "partitions_def": partition_daily
    },
    {
        "name": "landing_pss_master",
        "selection": AssetSelection.groups("pss_awo_master") & AssetSelection.key_prefixes("landings"),
        "partitions_def": partition_daily
    },
    {
        "name": "landing_pss_pkb",
        "selection": AssetSelection.groups("pss_awo_pkb") & AssetSelection.key_prefixes("landings"),
        "partitions_def": partition_so_daily
    },
    {
        "name": "landing_pss_sales",
        "selection": 
            AssetSelection.assets(AssetKey(["landings", "awo_transaction_sales_pss_staging"]))
            | AssetSelection.assets(AssetKey(["landings", "awo_transaction_sales_salesorder_pss_staging"])),
        "partitions_def": partition_so_daily
    },
    {
        "name": "landing_pss_sales_invoice",
        "selection": 
            AssetSelection.assets(AssetKey(["landings", "awo_transaction_sales_invoice_pss_staging"])),
        "partitions_def": partition_4so_daily
    },
    {
        "name": "landing_pss_sales_tvc",
        "selection": 
            AssetSelection.assets(AssetKey(["landings", "awo_transaction_sales_tvc_pss_staging"])),
        "partitions_def": partition_2so_daily
    },
    {
        "name": "landing_pss_sales_partnerdetail",
        "selection": 
            AssetSelection.assets(AssetKey(["landings", "awo_transaction_sales_invoice_tso_pss_staging"]))
            | AssetSelection.assets(AssetKey(["landings", "awo_transaction_sales_partnerdetail_pss_staging"])),
        "partitions_def": partition_daily
    },

    # INTEGRATION
    {
        "name": "integration_pss",
        "selection": 
            AssetSelection.groups("pss_awo_customer") & AssetSelection.key_prefixes("integrations")
            | AssetSelection.groups("pss_awo_master") & AssetSelection.key_prefixes("integrations")
            | AssetSelection.groups("pss_awo_pkb") & AssetSelection.key_prefixes("integrations")
            | AssetSelection.groups("pss_awo_sales") & AssetSelection.key_prefixes("integrations"),
        "partitions_def": partition_daily
    },
]

for config in job_configs:
    job = define_asset_job(
        name=f"job_{config['name']}", 
        selection=config["selection"],
        partitions_def=config["partitions_def"]
    )
    jobs.append(job)
    dicts[config["name"]] = job