from shared.utils.custom_asset import make_sling_asset_with_partition
from shared.partitions import partition_hourly


slings = [
    {
        "name": "sling_outbound_adm",
        "partitions_def": partition_hourly,
        "files": [
            "outbound_adm/cc_master_category.yaml",
            "outbound_adm/tms_prospect_campaign_result.yaml",
            "outbound_adm/tms_prospect_detail.yaml",
            "outbound_adm/tms_prospect_history_contact.yaml",
            "outbound_adm/tms_prospect_time_frame.yaml",
            "outbound_adm/tms_prospect.yaml",
        ],
    },
    {
        "name": "sling_outbound_ahm",
        "partitions_def": partition_hourly,
        "files": [
            "outbound_ahm/cc_master_category.yaml",
            "outbound_ahm/tms_prospect_campaign_result.yaml",
            "outbound_ahm/tms_prospect_detail.yaml",
            "outbound_ahm/tms_prospect_history_contact.yaml",
            "outbound_ahm/tms_prospect_time_frame.yaml",
            "outbound_ahm/tms_prospect.yaml",
        ],
    },
    {
        "name": "sling_outbound_esvi",
        "partitions_def": partition_hourly,
        "files": [
            "outbound_esvi/cc_master_category.yaml",
            "outbound_esvi/tms_prospect_campaign_result.yaml",
            "outbound_esvi/tms_prospect_detail.yaml",
            "outbound_esvi/tms_prospect_history_contact.yaml",
            "outbound_esvi/tms_prospect_time_frame.yaml",
            "outbound_esvi/tms_prospect.yaml",
        ],
    },
    {
        "name": "sling_outbound_mrs_iso",
        "partitions_def": partition_hourly,
        "files": [
            "outbound_mrs_iso/tms_master_category.yaml",
            "outbound_mrs_iso/tms_prospect_campaign_result.yaml",
            "outbound_mrs_iso/tms_prospect_detail.yaml",
            "outbound_mrs_iso/tms_prospect_response_detail.yaml",
            "outbound_mrs_iso/tms_prospect.yaml",
        ],
    },
    {
        "name": "sling_outbound_mrsdso",
        "partitions_def": partition_hourly,
        "files": [
            "outbound_mrsdso/cc_master_category.yaml",
            "outbound_mrsdso/tms_prospect_campaign_result.yaml",
            "outbound_mrsdso/tms_prospect_detail.yaml",
            "outbound_mrsdso/tms_prospect_history_contact.yaml",
            "outbound_mrsdso/tms_prospect_time_frame.yaml",
            "outbound_mrsdso/tms_prospect.yaml",
        ],
    },
    {
        "name": "sling_outbound_tafteleacquisition",
        "partitions_def": partition_hourly,
        "files": [
            "outbound_tafteleacquisition/cc_master_category.yaml",
            "outbound_tafteleacquisition/tms_prospect_campaign_result.yaml",
            "outbound_tafteleacquisition/tms_prospect_detail.yaml",
            "outbound_tafteleacquisition/tms_prospect_history_contact.yaml",
            "outbound_tafteleacquisition/tms_prospect_time_frame.yaml",
            "outbound_tafteleacquisition/tms_prospect.yaml",
        ],
    },
    {
        "name": "sling_outbound_deskcollfif",
        "partitions_def": partition_hourly,
        "files": [
            "outbound_deskcollfif/acs_call_history_daily.yaml",
            "outbound_deskcollfif/acs_call_history_detail_daily.yaml",
            "outbound_deskcollfif/acs_customer_data.yaml",
            "outbound_deskcollfif/acs_customer_profile_ext.yaml",
            "outbound_deskcollfif/acs_reference.yaml",
        ],
    },
    {
        "name": "sling_outbound_deskcolltaf",
        "partitions_def": partition_hourly,
        "files": [
            "outbound_deskcolltaf/acs_call_history_daily.yaml",
            "outbound_deskcolltaf/acs_call_history_detail_daily.yaml",
            "outbound_deskcolltaf/acs_class.yaml",
            "outbound_deskcolltaf/acs_customer_profile_ext.yaml",
            "outbound_deskcolltaf/acs_payment_today.yaml",
            "outbound_deskcolltaf/acs_reference.yaml",
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
