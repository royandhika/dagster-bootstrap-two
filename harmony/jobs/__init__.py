from dagster import AssetSelection, define_asset_job
from shared.partitions import partition_hourly, partition_2hourly, partition_3hourly, partition_12hourly, partition_daily


jobs = []
dicts = {}

job_configs = [
    {
        "prefix": "datalanding",
        "category": AssetSelection.key_prefixes("landings"),
        "assets": [
            {
                "partitions_def": partition_hourly,
                "selection": [
                    {
                        "name": "ecentrix_alpha", 
                        "select": AssetSelection.groups("ecentrix_alpha")
                    },
                    {
                        "name": "ecentrix_bravo", 
                        "select": AssetSelection.groups("ecentrix_bravo")
                    },
                    {
                        "name": "ecentrix_predictive", 
                        "select": AssetSelection.groups("ecentrix_predictive")
                    },
                    {
                        "name": "outbound_adm", 
                        "select": AssetSelection.groups("outbound_adm")
                    },
                    {
                        "name": "outbound_ahm", 
                        "select": AssetSelection.groups("outbound_ahm")
                    },
                    {
                        "name": "outbound_esvi", 
                        "select": AssetSelection.groups("outbound_esvi")
                    },
                    {
                        "name": "outbound_mrs_iso", 
                        "select": AssetSelection.groups("outbound_mrs_iso")
                    },
                    {
                        "name": "outbound_mrsdso", 
                        "select": AssetSelection.groups("outbound_mrsdso")
                    },
                    {
                        "name": "outbound_tafteleacquisition", 
                        "select": AssetSelection.groups("outbound_tafteleacquisition")
                    },
                    {
                        "name": "outbound_deskcollfif", 
                        "select": AssetSelection.groups("outbound_deskcollfif")
                    },
                    {
                        "name": "outbound_deskcolltaf", 
                        "select": AssetSelection.groups("outbound_deskcolltaf")
                    },
                ],
            },
            {
                "partitions_def": partition_2hourly,
                "selection": [
                    {
                        "name": "inbound_awda", 
                        "select": AssetSelection.groups("inbound_awda")
                    },
                    {
                        "name": "inbound_awo", 
                        "select": AssetSelection.groups("inbound_awo")
                    },
                    {
                        "name": "inbound_nasmoco", 
                        "select": AssetSelection.groups("inbound_nasmoco")
                    },
                    {
                        "name": "inbound_omni_astralife", 
                        "select": AssetSelection.groups("inbound_omni_astralife")
                    },
                    {
                        "name": "inbound_shopanddrive_v4", 
                        "select": AssetSelection.groups("inbound_shopanddrive_v4")
                    },
                    {
                        "name": "inbound_taf", 
                        "select": AssetSelection.groups("inbound_taf")
                    },
                    {
                        "name": "inbound_trac", 
                        "select": AssetSelection.groups("inbound_trac")
                    },
                ]
            },
            {
                "partitions_def": partition_12hourly,
                "selection": [
                    {
                        "name": "outbound_clipan_duitcair", 
                        "select": AssetSelection.groups("outbound_clipan_duitcair")
                    },
                    {
                        "name": "outbound_tam_concierge", 
                        "select": AssetSelection.groups("outbound_tam_concierge")
                    },
                    {
                        "name": "outbound_jmfi_mycash", 
                        "select": AssetSelection.groups("outbound_jmfi_mycash")
                    },
                ]
            },
        ]
    },
    {
        "prefix": "datamart",
        "category": AssetSelection.key_prefixes("marts"),
        "assets": [
            {
                "partitions_def": partition_3hourly,
                "selection": [
                    {
                        "name": "outbound_tafteleacquisition", 
                        "select": AssetSelection.groups("outbound_tafteleacquisition")
                    },
                    {
                        "name": "outbound_deskcolltaf", 
                        "select": AssetSelection.groups("outbound_deskcolltaf")
                    },
                ],
            },
            {
                "partitions_def": partition_12hourly,
                "selection": [
                    {
                        "name": "ecentrix_alpha", 
                        "select": AssetSelection.groups("ecentrix_alpha")
                    },
                    {
                        "name": "ecentrix_bravo", 
                        "select": AssetSelection.groups("ecentrix_bravo")
                    },
                    {
                        "name": "ecentrix_predictive", 
                        "select": AssetSelection.groups("ecentrix_predictive")
                    },
                    {
                        "name": "outbound_adm", 
                        "select": AssetSelection.groups("outbound_adm")
                    },
                    {
                        "name": "outbound_ahm", 
                        "select": AssetSelection.groups("outbound_ahm")
                    },
                    {
                        "name": "outbound_esvi", 
                        "select": AssetSelection.groups("outbound_esvi")
                    },
                    {
                        "name": "outbound_mrs_iso", 
                        "select": AssetSelection.groups("outbound_mrs_iso")
                    },
                    {
                        "name": "outbound_mrsdso", 
                        "select": AssetSelection.groups("outbound_mrsdso")
                    },
                    {
                        "name": "outbound_deskcollfif", 
                        "select": AssetSelection.groups("outbound_deskcollfif")
                    },
                    {
                        "name": "outbound_clipan_duitcair", 
                        "select": AssetSelection.groups("outbound_clipan_duitcair")
                    },
                    {
                        "name": "outbound_tam_concierge", 
                        "select": AssetSelection.groups("outbound_tam_concierge")
                    },
                    {
                        "name": "outbound_jmfi_mycash", 
                        "select": AssetSelection.groups("outbound_jmfi_mycash")
                    },
                ],
            },
            {
                "partitions_def": partition_daily,
                "selection": [
                    {
                        "name": "inbound_awda", 
                        "select": AssetSelection.groups("inbound_awda")
                    },
                    {
                        "name": "inbound_awo", 
                        "select": AssetSelection.groups("inbound_awo")
                    },
                    {
                        "name": "inbound_nasmoco", 
                        "select": AssetSelection.groups("inbound_nasmoco")
                    },
                    {
                        "name": "inbound_taf", 
                        "select": AssetSelection.groups("inbound_taf")
                    },
                    {
                        "name": "inbound_trac", 
                        "select": AssetSelection.groups("inbound_trac")
                    },
                ]
            }
        ],
    }
]

for config in job_configs:
    for schedule in config["assets"]:
        for group in schedule["selection"]:
            job = define_asset_job(
                name=f"job_{config['prefix']}_{group['name']}",
                selection=config["category"] & group["select"],
                partitions_def=schedule["partitions_def"]
            )
            jobs.append(job)
            dicts[config['prefix'] + "_" + group["name"]] = job