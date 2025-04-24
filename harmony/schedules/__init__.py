# from dagster import build_schedule_from_partitioned_job, ScheduleDefinition
# from jobs import jobs

# schedule_outbound_mrsdso = build_schedule_from_partitioned_job(job_outbound_mrsdso)

# schedule_ecentrix_alpha = build_schedule_from_partitioned_job(job_ecentrix_alpha)

# schedules = [
#     build_schedule_from_partitioned_job(job=config[""])
# ]
# schedules = []
# for job in jobs:
#     schedule = ScheduleDefinition(
#         job=job,
#         cron_schedule=
#     )