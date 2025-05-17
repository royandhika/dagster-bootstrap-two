from dagster import build_schedule_from_partitioned_job
from jobs import dicts


schedules = []

for job_name, job in dicts.items():
    if "datalanding" in job_name: 
        schedule = build_schedule_from_partitioned_job(
            job=job,
            name=f"schedule_{job_name}"
        )
        schedules.append(schedule)
