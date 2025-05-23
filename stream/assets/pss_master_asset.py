from shared.partitions import partition_daily
from shared.utils.custom_asset import make_pss_asset_with_partition
import pss.master as master


masters = {
    "group": "pss_awo_master",
    "partitions_def": partition_daily,
    "tables": [
        {
            "name": "awo_detail_activitytype_pss_staging",
            "query": master.AWO_DETAIL_ACTIVITYTYPE
        },
        {
            "name": "awo_detail_billingtype_pss_staging",
            "query": master.AWO_DETAIL_BILLINGTYPE
        },
        {
            "name": "awo_detail_chargeto_pss_staging",
            "query": master.AWO_DETAIL_CHARGETO
        },
        {
            "name": "awo_detail_city_pss_staging",
            "query": master.AWO_DETAIL_CITY
        },
        {
            "name": "awo_detail_dealer_businessarea_pss_staging",
            "query": master.AWO_DETAIL_DEALER_BUSINESSAREA
        },
        {
            "name": "awo_detail_dealer_salesoffice_pss_staging",
            "query": master.AWO_DETAIL_DEALER_SALESOFFICE
        },
        {
            "name": "awo_detail_employee_pss_staging",
            "query": master.AWO_DETAIL_EMPLOYEE
        },
        {
            "name": "awo_detail_jobtype_pss_staging",
            "query": master.AWO_DETAIL_JOBTYPE
        },
        {
            "name": "awo_detail_partnerfunction_pss_staging",
            "query": master.AWO_DETAIL_PARTNERFUNCTION
        },
        {
            "name": "awo_detail_pkbtype_pss_staging",
            "query": master.AWO_DETAIL_PKBTYPE
        },
        {
            "name": "awo_detail_position_pss_staging",
            "query": master.AWO_DETAIL_POSITION
        },
        {
            "name": "awo_detail_region_pss_staging",
            "query": master.AWO_DETAIL_REGION
        },
    ]
}

for table in masters["tables"]:
    globals()[table["name"]] = make_pss_asset_with_partition(
        name=table["name"],
        group=masters["group"],
        partitions_def=masters["partitions_def"],
        query=table["query"]
    )