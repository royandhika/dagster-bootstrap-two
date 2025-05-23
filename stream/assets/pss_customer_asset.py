from shared.partitions import partition_daily
from shared.utils.custom_asset import make_pss_asset_with_partition
import pss.customer as customer


customers = {
    "group": "pss_awo_customer",
    "partitions_def": partition_daily,
    "tables": [
        {
            "name": "awo_customer_pss_staging",
            "query": customer.AWO_CUSTOMER
        },
        {
            "name": "awo_customer_address_pss_staging",
            "query": customer.AWO_CUSTOMER_ADDRESS
        },
        {
            "name": "awo_customer_addressusage_pss_staging",
            "query": customer.AWO_CUSTOMER_ADDRESSUSAGE
        },
        {
            "name": "awo_customer_email_pss_staging",
            "query": customer.AWO_CUSTOMER_EMAIL
        },
        {
            "name": "awo_customer_fax_pss_staging",
            "query": customer.AWO_CUSTOMER_FAX
        },
        {
            "name": "awo_customer_flat_pss_staging",
            "query": customer.AWO_CUSTOMER_FLAT
        },
        {
            "name": "awo_customer_marketingattribute_pss_staging",
            "query": customer.AWO_CUSTOMER_MARKETINGATTRIBUTE
        },
        {
            "name": "awo_customer_telephone_pss_staging",
            "query": customer.AWO_CUSTOMER_TELEPHONE
        },
    ]
}

for table in customers["tables"]:
    globals()[table["name"]] = make_pss_asset_with_partition(
        name=table["name"],
        group=customers["group"],
        partitions_def=customers["partitions_def"],
        query=table["query"]
    )