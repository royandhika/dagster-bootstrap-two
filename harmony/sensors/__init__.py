from dagster import run_status_sensor, RunRequest, DagsterRunStatus
from jobs import dicts
from datetime import datetime


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dicts["tms_datalanding_hourly"]],
    request_jobs=[dicts["tms_datamart_3hourly"]],
    minimum_interval_seconds=60,
)
def sensor_tms_datamart_3hourly(context):
    if datetime.now().hour % 3 == 0:
        partition_keys = dicts["tms_datamart_3hourly"].partitions_def.get_partition_keys()
        last_partition = partition_keys[-1]
        yield RunRequest(partition_key=last_partition)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dicts["tms_datalanding_hourly"], dicts["tms_datalanding_2hourly"]],
    request_jobs=[dicts["tms_datamart_12hourly"]],
    minimum_interval_seconds=60,
)
def sensor_tms_datamart_12hourly(context):
    if datetime.now().hour % 12 == 0:
        partition_keys = dicts["tms_datamart_12hourly"].partitions_def.get_partition_keys()
        last_partition = partition_keys[-1]
        yield RunRequest(partition_key=last_partition)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dicts["api_datalanding_12hourly"]],
    request_jobs=[dicts["api_datamart_12hourly"]],
    minimum_interval_seconds=60,
)
def sensor_api_datamart_12hourly(context):
    if datetime.now().hour % 12 == 0:
        partition_keys = dicts["api_datamart_12hourly"].partitions_def.get_partition_keys()
        last_partition = partition_keys[-1]
        yield RunRequest(partition_key=last_partition)


sensors = [
    sensor_tms_datamart_3hourly,
    sensor_tms_datamart_12hourly,
    sensor_api_datamart_12hourly,
]
