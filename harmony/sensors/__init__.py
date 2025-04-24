from dagster import run_status_sensor, RunRequest, DagsterRunStatus
from jobs import dicts
from datetime import datetime


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dicts["outbound_datalanding"]],
    request_jobs=[dicts["outbound_datamart"]],
    minimum_interval_seconds=30,
)
def sensor_outbound_datamart(context):
    if datetime.now().hour % 6 == 0:
        partition_keys = dicts["outbound_datamart"].partitions_def.get_partition_keys()
        last_partition = partition_keys[-1]
        yield RunRequest(partition_key=last_partition)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dicts["telephony_datalanding"]],
    request_jobs=[dicts["telephony_datamart"]],
    minimum_interval_seconds=30,
)
def sensor_telephony_datamart(context):
    if datetime.now().hour % 6 == 0:
        partition_keys = dicts["telephony_datamart"].partitions_def.get_partition_keys()
        last_partition = partition_keys[-1]
        yield RunRequest(partition_key=last_partition)


@run_status_sensor(
    run_status=DagsterRunStatus.SUCCESS,
    monitored_jobs=[dicts["inbound_datalanding"]],
    request_jobs=[dicts["inbound_datamart"]],
    minimum_interval_seconds=30,
)
def sensor_inbound_datamart(context):
    if datetime.now().hour % 8 == 0:
        partition_keys = dicts["inbound_datamart"].partitions_def.get_partition_keys()
        last_partition = partition_keys[-1]
        yield RunRequest(partition_key=last_partition)


sensors = [
    sensor_outbound_datamart,
    sensor_telephony_datamart,
    sensor_inbound_datamart,
]
