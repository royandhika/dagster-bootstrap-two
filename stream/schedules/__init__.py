from dagster import build_schedule_from_partitioned_job
from jobs.job_pss import dicts


schedules = []

for job_name, job in dicts.items():
    if "landing" in job_name: 
        schedule = build_schedule_from_partitioned_job(
            job=job,
            name=f"schedule_{job_name}"
        )
        schedules.append(schedule)
