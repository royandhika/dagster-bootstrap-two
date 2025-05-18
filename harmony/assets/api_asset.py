from dagster import asset, AssetExecutionContext, MaterializeResult, EnvVar, MetadataValue, BackfillPolicy
from shared.resources import APIResource
from shared.partitions import partition_12hourly
from datetime import timedelta
from shared.utils.custom_translator import CustomPandasRun
import inspect


@asset(
    partitions_def=partition_12hourly,
    backfill_policy=BackfillPolicy.single_run(),
    pool="api_crm",
    key_prefix=["landings"],
    group_name="outbound_clipan_duitcair",
    kinds={"sqlserver", "python"},
    metadata={
        "dagster/table_name": f"AWODB.{EnvVar('ENV_SCHEMA').get_value()}_dl.outbound_clipan_duitcair_report_prospect_api",
    },
)
def outbound_clipan_duitcair_report_prospect_api(context: AssetExecutionContext, api_crm: APIResource, config: CustomPandasRun):
    project = "clipan_duitcair"
    url = "https://api.astraworld.info/clipan/api/v2/supply/last-response"
    start, end = context.partition_time_window
    start -= timedelta(minutes=30)
    
    types = ["call_date", "data_supply_date"]
    count = 0
    for type in types:
        payload = {
            "campaign_id": "9d33d466-f4c9-44eb-8225-f7855d0e3a97",
            f"{type}_from": f"{start.strftime('%Y-%m-%d %H:%M:%S')}",
            f"{type}_to": f"{end.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        data = api_crm.request(
            project=project,
            url=url,
            table_name=inspect.currentframe().f_code.co_name, # type: ignore
            pandas_method=config.method,
            payload=payload,
        )

        count += len(data)

    yield MaterializeResult(
        metadata={
            "dagster/row_count": MetadataValue.int(count)
        }
    )


@asset(
    partitions_def=partition_12hourly,
    backfill_policy=BackfillPolicy.single_run(),
    pool="api_crm",
    key_prefix=["landings"],
    group_name="outbound_clipan_duitcair",
    kinds={"sqlserver", "python"},
    metadata={
        "dagster/table_name": f"AWODB.{EnvVar('ENV_SCHEMA').get_value()}_dl.outbound_clipan_duitcair_report_prospect_timeframe_api",
    },
)
def outbound_clipan_duitcair_report_prospect_timeframe_api(context: AssetExecutionContext, api_crm: APIResource, config: CustomPandasRun):
    project = "clipan_duitcair"
    url = "https://api.astraworld.info/clipan/api/v2/supply/time-frame"
    start, end = context.partition_time_window
    start -= timedelta(minutes=30)
    
    types = ["call_date"]
    count = 0
    for type in types:
        payload = {
            "campaign_id": "9d33d466-f4c9-44eb-8225-f7855d0e3a97",
            f"{type}_from": f"{start.strftime('%Y-%m-%d %H:%M:%S')}",
            f"{type}_to": f"{end.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        data = api_crm.request(
            project=project,
            url=url,
            table_name=inspect.currentframe().f_code.co_name, # type: ignore
            pandas_method=config.method,
            payload=payload,
        )

        count += len(data)

    yield MaterializeResult(
        metadata={
            "dagster/row_count": MetadataValue.int(count)
        }
    )


@asset(
    partitions_def=partition_12hourly,
    backfill_policy=BackfillPolicy.single_run(),
    pool="api_crm",
    key_prefix=["landings"],
    group_name="outbound_tam_concierge",
    kinds={"sqlserver", "python"},
    metadata={
        "dagster/table_name": f"AWODB.{EnvVar('ENV_SCHEMA').get_value()}_dl.outbound_tam_concierge_report_prospect_api",
    },
)
def outbound_tam_concierge_report_prospect_api(context: AssetExecutionContext, api_crm: APIResource, config: CustomPandasRun):
    project = "tam_concierge"
    url = "https://api.astraworld.info/tam/api/v2/supply/last-response"
    start, end = context.partition_time_window
    start -= timedelta(minutes=30)
    
    types = ["call_date", "data_supply_date"]
    count = 0
    for type in types:
        payload = {
            "campaign_id": "9ced60dd-5f58-48d2-ad27-63bfc12ce424",
            f"{type}_from": f"{start.strftime('%Y-%m-%d %H:%M:%S')}",
            f"{type}_to": f"{end.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        data = api_crm.request(
            project=project,
            url=url,
            table_name=inspect.currentframe().f_code.co_name, # type: ignore
            pandas_method=config.method,
            payload=payload,
        )

        count += len(data)

    yield MaterializeResult(
        metadata={
            "dagster/row_count": MetadataValue.int(count)
        }
    )


@asset(
    partitions_def=partition_12hourly,
    backfill_policy=BackfillPolicy.single_run(),
    pool="api_crm",
    key_prefix=["landings"],
    group_name="outbound_tam_concierge",
    kinds={"sqlserver", "python"},
    metadata={
        "dagster/table_name": f"AWODB.{EnvVar('ENV_SCHEMA').get_value()}_dl.outbound_tam_concierge_report_prospect_timeframe_api",
    },
)
def outbound_tam_concierge_report_prospect_timeframe_api(context: AssetExecutionContext, api_crm: APIResource, config: CustomPandasRun):
    project = "tam_concierge"
    url = "https://api.astraworld.info/tam/api/v2/supply/time-frame"
    start, end = context.partition_time_window
    start -= timedelta(minutes=30)
    
    types = ["call_date"]
    count = 0
    for type in types:
        payload = {
            "campaign_id": "9ced60dd-5f58-48d2-ad27-63bfc12ce424",
            f"{type}_from": f"{start.strftime('%Y-%m-%d %H:%M:%S')}",
            f"{type}_to": f"{end.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        data = api_crm.request(
            project=project,
            url=url,
            table_name=inspect.currentframe().f_code.co_name, # type: ignore
            pandas_method=config.method,
            payload=payload,
        )

        count += len(data)

    yield MaterializeResult(
        metadata={
            "dagster/row_count": MetadataValue.int(count)
        }
    )


@asset(
    partitions_def=partition_12hourly,
    backfill_policy=BackfillPolicy.single_run(),
    pool="api_crm",
    key_prefix=["landings"],
    group_name="outbound_jmfi_mycash",
    kinds={"sqlserver", "python"},
    metadata={
        "dagster/table_name": f"AWODB.{EnvVar('ENV_SCHEMA').get_value()}_dl.outbound_jmfi_mycash_report_prospect_api",
    },
)
def outbound_jmfi_mycash_report_prospect_api(context: AssetExecutionContext, api_crm: APIResource, config: CustomPandasRun):
    project = "jmfi_mycash"
    url = "https://api-jmfi.astraworld.info/api/v1/interaction"
    start, end = context.partition_time_window
    start -= timedelta(minutes=30)
    
    types = ["4W", "2W"]
    count = 0
    for type in types:
        payload = {
            "campaign": f"{type}",
            "type" : "last_response",
            "interaction_date_from": f"{start.strftime('%Y-%m-%d %H:%M:%S')}",
            "interaction_date_to": f"{end.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        data = api_crm.request(
            project=project,
            url=url,
            table_name=inspect.currentframe().f_code.co_name, # type: ignore
            pandas_method=config.method,
            payload=payload,
        )

        count += len(data)

    yield MaterializeResult(
        metadata={
            "dagster/row_count": MetadataValue.int(count)
        }
    )


@asset(
    partitions_def=partition_12hourly,
    backfill_policy=BackfillPolicy.single_run(),
    pool="api_crm",
    key_prefix=["landings"],
    group_name="outbound_jmfi_mycash",
    kinds={"sqlserver", "python"},
    metadata={
        "dagster/table_name": f"AWODB.{EnvVar('ENV_SCHEMA').get_value()}_dl.outbound_jmfi_mycash_report_prospect_timeframe_api",
    },
)
def outbound_jmfi_mycash_report_prospect_timeframe_api(context: AssetExecutionContext, api_crm: APIResource, config: CustomPandasRun):
    project = "jmfi_mycash"
    url = "https://api-jmfi.astraworld.info/api/v1/interaction"
    start, end = context.partition_time_window
    start -= timedelta(minutes=30)
    
    types = ["4W", "2W"]
    count = 0
    for type in types:
        payload = {
            "campaign": f"{type}",
            "type" : "time_frame",
            "interaction_date_from": f"{start.strftime('%Y-%m-%d %H:%M:%S')}",
            "interaction_date_to": f"{end.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        data = api_crm.request(
            project=project,
            url=url,
            table_name=inspect.currentframe().f_code.co_name, # type: ignore
            pandas_method=config.method,
            payload=payload,
        )

        count += len(data)

    yield MaterializeResult(
        metadata={
            "dagster/row_count": MetadataValue.int(count)
        }
    )


@asset(
    partitions_def=partition_12hourly,
    backfill_policy=BackfillPolicy.single_run(),
    pool="api_crm",
    key_prefix=["landings"],
    group_name="outbound_jmfi_mycash",
    kinds={"sqlserver", "python"},
    metadata={
        "dagster/table_name": f"AWODB.{EnvVar('ENV_SCHEMA').get_value()}_dl.outbound_jmfi_mycash_golive_api",
    },
)
def outbound_jmfi_mycash_golive_api(context: AssetExecutionContext, api_crm: APIResource, config: CustomPandasRun):
    project = "jmfi_mycash"
    url = "https://api-jmfi.astraworld.info/api/v1/go-live"
    start, end = context.partition_time_window
    start -= timedelta(minutes=30)
    
    types = ["4W", "2W"]
    count = 0
    for type in types:
        payload = {
            "campaign": f"{type}",
            "last_go_live_update_date_from": f"{start.strftime('%Y-%m-%d %H:%M:%S')}",
            "last_go_live_update_date_to": f"{end.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        data = api_crm.request(
            project=project,
            url=url,
            table_name=inspect.currentframe().f_code.co_name, # type: ignore
            pandas_method=config.method,
            payload=payload,
        )

        count += len(data)

    yield MaterializeResult(
        metadata={
            "dagster/row_count": MetadataValue.int(count)
        }
    )


@asset(
    partitions_def=partition_12hourly,
    backfill_policy=BackfillPolicy.single_run(),
    pool="api_crm",
    key_prefix=["landings"],
    group_name="outbound_jmfi_mycash",
    kinds={"sqlserver", "python"},
    metadata={
        "dagster/table_name": f"AWODB.{EnvVar('ENV_SCHEMA').get_value()}_dl.outbound_jmfi_mycash_supply_api",
    },
)
def outbound_jmfi_mycash_supply_api(context: AssetExecutionContext, api_crm: APIResource, config: CustomPandasRun):
    project = "jmfi_mycash"
    url = "https://api-jmfi.astraworld.info/api/v1/supply"
    start, end = context.partition_time_window
    start -= timedelta(minutes=30)
    
    types = ["4W", "2W"]
    count = 0
    for type in types:
        payload = {
            "campaign": f"{type}",
            "assignment_date_from": f"{start.strftime('%Y-%m-%d %H:%M:%S')}",
            "assignment_date_to": f"{end.strftime('%Y-%m-%d %H:%M:%S')}",
            "supply_date_from": f"{start.strftime('%Y-%m-%d %H:%M:%S')}",
            "supply_date_to": f"{end.strftime('%Y-%m-%d %H:%M:%S')}"
        }

        data = api_crm.request(
            project=project,
            url=url,
            table_name=inspect.currentframe().f_code.co_name, # type: ignore
            pandas_method=config.method,
            payload=payload,
        )

        count += len(data)

    yield MaterializeResult(
        metadata={
            "dagster/row_count": MetadataValue.int(count)
        }
    )