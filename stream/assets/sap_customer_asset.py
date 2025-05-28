from shared.partitions.static_so import partition_5so_daily 
from shared.utils.custom_asset import make_sap_asset_with_partition
import sap
    

customers = {
    "group": "sap_awo_customer",
    "partitions_def": partition_5so_daily,
    "lead": 1,
    "tables": [
        {
            "name": "awo_customer_sap_staging",
            "file": sap.AWO_CUSTOMER
        },
        {
            "name": "awo_customer_address_sap_staging",
            "file": sap.AWO_CUSTOMER_ADDRESS
        },
        {
            "name": "awo_customer_addressusage_sap_staging",
            "file": sap.AWO_CUSTOMER_ADDRESSUSAGE
        },
        {
            "name": "awo_customer_contact_sap_staging",
            "file": sap.AWO_CUSTOMER_CONTACT
        },
        {
            "name": "awo_customer_fax_sap_staging",
            "file": sap.AWO_CUSTOMER_FAX
        },
        {
            "name": "awo_customer_marketingattribute_sap_staging",
            "file": sap.AWO_CUSTOMER_MARKETINGATTRIBUTE
        },
        {
            "name": "awo_customer_salesarea_sap_staging",
            "file": sap.AWO_CUSTOMER_SALESAREA
        },
    ]
}

for table in customers["tables"]:
    globals()[table["name"]] = make_sap_asset_with_partition(
        name=table["name"],
        group=customers["group"],
        partitions_def=customers["partitions_def"],
        file_name=table["file"],
        lead=customers["lead"],
    )