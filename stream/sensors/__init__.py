from shared.utils.custom_asset import make_dbt_sensor_with_multiple_required
from jobs.job_pss import dicts


sensors = []

sensor_configs = [
    {
        "name": "sensor_integration_pss",
        "monitored_job": [
            dicts["landing_pss_customer"],
            dicts["landing_pss_master"],
            dicts["landing_pss_pkb"],
            dicts["landing_pss_sales"],
            dicts["landing_pss_sales_invoice"],
            dicts["landing_pss_sales_tvc"],
            dicts["landing_pss_sales_partnerdetail"],
        ],
        "request_job": dicts["integration_pss"],
        "range": 12,
    }
]

for config in sensor_configs:
    sensor = make_dbt_sensor_with_multiple_required(
        name=config["name"],
        monitored_jobs=config["monitored_job"],
        request_job=config["request_job"],
        lookback_range=config["range"]
    )
    sensors.append(sensor)