from dagster import AssetSelection, define_asset_job, build_schedule_from_partitioned_job
from shared.partitions import partition_hourly, partition_2hourly, partition_3hourly, partition_12hourly, partition_daily


jobs = []
schedules = []
dicts = {}

job_configs = [
    {
        "name": "tms_datalanding_hourly",
        "selection": 
            AssetSelection.groups("ecentrix_alpha") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("ecentrix_bravo") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("ecentrix_predictive") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("outbound_adm") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("outbound_ahm") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("outbound_esvi") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("outbound_mrs_iso") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("outbound_mrsdso") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("outbound_tafteleacquisition") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("outbound_deskcollfif") & AssetSelection.key_prefixes("landings"),
        "partitions_def": partition_hourly
    },
    {
        "name": "tms_datalanding_2hourly",
        "selection":
            AssetSelection.groups("inbound_awda") & AssetSelection.key_prefixes("landings") 
            | AssetSelection.groups("inbound_awo") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("inbound_nasmoco") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("inbound_omni_astralife") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("inbound_shopanddrive_v4") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("inbound_taf") & AssetSelection.key_prefixes("landings"),
        "partitions_def": partition_2hourly
    },
    {
        "name": "api_datalanding_12hourly",
        "selection":
            AssetSelection.groups("outbound_clipan_duitcair") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("outbound_tam_concierge") & AssetSelection.key_prefixes("landings")
            | AssetSelection.groups("outbound_jmfi_mycash") & AssetSelection.key_prefixes("landings"),
        "partitions_def": partition_12hourly
    },
    
    {
        "name": "tms_datamart_3hourly",
        "selection":
            AssetSelection.groups("outbound_tafteleacquisition") & AssetSelection.key_prefixes("marts"),
        "partitions_def": partition_3hourly
    },
    {
        "name": "tms_datamart_12hourly",
        "selection": 
            AssetSelection.groups("ecentrix_alpha") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("ecentrix_bravo") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("ecentrix_predictive") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("outbound_adm") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("outbound_ahm") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("outbound_esvi") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("outbound_mrs_iso") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("outbound_mrsdso") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("outbound_deskcollfif") & AssetSelection.key_prefixes("marts"),
        "partitions_def": partition_12hourly            
    },
    {
        "name": "tms_datamart_daily",
        "selection": 
            AssetSelection.groups("inbound_awda") & AssetSelection.key_prefixes("marts") 
            | AssetSelection.groups("inbound_awo") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("inbound_nasmoco") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("inbound_taf") & AssetSelection.key_prefixes("marts"),
        "partitions_def": partition_daily
    },
    {
        "name": "api_datamart_12hourly",
        "selection": 
            AssetSelection.groups("outbound_clipan_duitcair") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("outbound_tam_concierge") & AssetSelection.key_prefixes("marts")
            | AssetSelection.groups("outbound_jmfi_mycash") & AssetSelection.key_prefixes("marts"),
        "partitions_def": partition_12hourly            
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
    
for job_name, job in dicts.items():
    if "landing" in job_name: 
        schedule = build_schedule_from_partitioned_job(
            job=job,
            name=f"schedule_{job_name}"
        )
        schedules.append(schedule)
