from dagster import DailyPartitionsDefinition, TimeWindowPartitionsDefinition
from datetime import datetime


partition_start_date = "2024-01-01"
partition_start_hour = "2024-01-01-00:00"
partition_start_datetime = datetime(2024, 1, 1)

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
partition_2hourly = TimeWindowPartitionsDefinition(
    start=partition_start_datetime,
    cron_schedule="0 */2 * * *",
    fmt="%Y-%m-%d-%H",
    timezone="Asia/Jakarta"
)
partition_3hourly = TimeWindowPartitionsDefinition(
    start=partition_start_datetime,
    cron_schedule="0 */3 * * *",
    fmt="%Y-%m-%d-%H",
    timezone="Asia/Jakarta"
)
partition_4hourly = TimeWindowPartitionsDefinition(
    start=partition_start_datetime,
    cron_schedule="0 */4 * * *",
    fmt="%Y-%m-%d-%H",
    timezone="Asia/Jakarta"
)
partition_12hourly = TimeWindowPartitionsDefinition(
    start=partition_start_datetime,
    cron_schedule="0 */12 * * *",
    fmt="%Y-%m-%d-%H",
    timezone="Asia/Jakarta"
)