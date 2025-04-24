from dagster import DailyPartitionsDefinition, TimeWindowPartitionsDefinition
from datetime import datetime

partition_start_date = "2023-01-01"
partition_start_hour = "2023-01-01-00:00"
partition_start_datetime = datetime(2023, 1, 1)

partition_daily = DailyPartitionsDefinition(
    start_date=partition_start_date, 
    timezone="Asia/Jakarta"
)
partition_hourly = TimeWindowPartitionsDefinition(
    start=partition_start_datetime,
    cron_schedule="0 * * * *",
    fmt="%Y-%m-%d-%H",
    timezone="Asia/Jakarta"
)
partition_2_hour = TimeWindowPartitionsDefinition(
    start=partition_start_datetime,
    cron_schedule="0 */2 * * *",
    fmt="%Y-%m-%d-%H",
    timezone="Asia/Jakarta"
)
partition_6_hour = TimeWindowPartitionsDefinition(
    start=partition_start_datetime,
    cron_schedule="0 */6 * * *",
    fmt="%Y-%m-%d-%H",
    timezone="Asia/Jakarta"
)
partition_8_hour = TimeWindowPartitionsDefinition(
    start=partition_start_datetime,
    cron_schedule="0 */8 * * *",
    fmt="%Y-%m-%d-%H",
    timezone="Asia/Jakarta"
)
partition_12_hour = TimeWindowPartitionsDefinition(
    start=partition_start_datetime,
    cron_schedule="0 */12 * * *",
    fmt="%Y-%m-%d-%H",
    timezone="Asia/Jakarta"
)