from dagster import DailyPartitionsDefinition


partition_start_date = "2022-01-01"

partition_daily = DailyPartitionsDefinition(start_date=partition_start_date)