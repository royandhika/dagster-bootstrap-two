from shared.utils.custom_asset import make_dbt_sensors_with_partition
from jobs import dicts


sensors = []

sensor_configs = [
    {
        "interval": 3,
        "sensors": [
            {
                "name": "sensor_outbound_tafteleacquisition",
                "monitored_job": dicts["datalanding_outbound_tafteleacquisition"],
                "request_job": dicts["datamart_outbound_tafteleacquisition"],
            },
            {
                "name": "sensor_outbound_deskcolltaf",
                "monitored_job": dicts["datalanding_outbound_deskcolltaf"],
                "request_job": dicts["datamart_outbound_deskcolltaf"],
            },
        ],
    },
    {
        "interval": 12,
        "sensors": [
            {
                "name": "sensor_ecentrix_alpha",
                "monitored_job": dicts["datalanding_ecentrix_alpha"],
                "request_job": dicts["datamart_ecentrix_alpha"],
            },
            {
                "name": "sensor_ecentrix_bravo",
                "monitored_job": dicts["datalanding_ecentrix_bravo"],
                "request_job": dicts["datamart_ecentrix_bravo"],
            },
            {
                "name": "sensor_ecentrix_predictive",
                "monitored_job": dicts["datalanding_ecentrix_predictive"],
                "request_job": dicts["datamart_ecentrix_predictive"],
            },
            {
                "name": "sensor_outbound_adm",
                "monitored_job": dicts["datalanding_outbound_adm"],
                "request_job": dicts["datamart_outbound_adm"],
            },
            {
                "name": "sensor_outbound_ahm",
                "monitored_job": dicts["datalanding_outbound_ahm"],
                "request_job": dicts["datamart_outbound_ahm"],
            },
            {
                "name": "sensor_outbound_esvi",
                "monitored_job": dicts["datalanding_outbound_esvi"],
                "request_job": dicts["datamart_outbound_esvi"],
            },
            {
                "name": "sensor_outbound_mrs_iso",
                "monitored_job": dicts["datalanding_outbound_mrs_iso"],
                "request_job": dicts["datamart_outbound_mrs_iso"],
            },
            {
                "name": "sensor_outbound_mrsdso",
                "monitored_job": dicts["datalanding_outbound_mrsdso"],
                "request_job": dicts["datamart_outbound_mrsdso"],
            },
            {
                "name": "sensor_outbound_deskcollfif",
                "monitored_job": dicts["datalanding_outbound_deskcollfif"],
                "request_job": dicts["datamart_outbound_deskcollfif"],
            },
            {
                "name": "sensor_outbound_clipan_duitcair",
                "monitored_job": dicts["datalanding_outbound_clipan_duitcair"],
                "request_job": dicts["datamart_outbound_clipan_duitcair"],
            },
            {
                "name": "sensor_outbound_tam_concierge",
                "monitored_job": dicts["datalanding_outbound_tam_concierge"],
                "request_job": dicts["datamart_outbound_tam_concierge"],
            },
            {
                "name": "sensor_outbound_jmfi_mycash",
                "monitored_job": dicts["datalanding_outbound_jmfi_mycash"],
                "request_job": dicts["datamart_outbound_jmfi_mycash"],
            },
        ],
    },
    {
        "interval": 24,
        "sensors": [
            {
                "name": "sensor_inbound_awda",
                "monitored_job": dicts["datalanding_inbound_awda"],
                "request_job": dicts["datamart_inbound_awda"],
            },
            {
                "name": "sensor_inbound_awo",
                "monitored_job": dicts["datalanding_inbound_awo"],
                "request_job": dicts["datamart_inbound_awo"],
            },
            {
                "name": "sensor_inbound_nasmoco",
                "monitored_job": dicts["datalanding_inbound_nasmoco"],
                "request_job": dicts["datamart_inbound_nasmoco"],
            },
            {
                "name": "sensor_inbound_taf",
                "monitored_job": dicts["datalanding_inbound_taf"],
                "request_job": dicts["datamart_inbound_taf"],
            },
        ],
    }
]
   
for schedule in sensor_configs:
    for config in schedule["sensors"]:
        sensor = make_dbt_sensors_with_partition(
            name=config["name"],
            monitored_jobs=[config["monitored_job"]],
            request_jobs=[config["request_job"]],
            interval=schedule["interval"]
        )
        sensors.append(sensor)