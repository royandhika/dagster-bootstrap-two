from dagster import AssetSpec, AssetKey, EnvVar
from datetime import datetime, timedelta
import yaml
import os

def create_external_asset(group: str, key: list, desc: str, kind: list) -> AssetSpec:
    return AssetSpec(
        key=AssetKey(key),
        description=desc,
        group_name=group,
        kinds=kind
    )

def sling_yaml_dict(filename: str) -> dict:
    env = EnvVar("ENV_SCHEMA").get_value()

    def inject_env(obj):
        if isinstance(obj, dict):
            return {k: inject_env(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [inject_env(v) for v in obj]
        elif isinstance(obj, str):
            return obj.replace("{ENV_SCHEMA}", env)
        return obj
    
    sling_dir = os.path.join(os.path.dirname(__file__), f"../../../app/sling")
    path = os.path.abspath(os.path.join(sling_dir, filename))
            
    with open(path, "r") as file:
        config = yaml.safe_load(file)

    return inject_env(config)

def sling_add_backfill(config: dict, start_date: str) -> dict:
    end_date = (datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    range_date = start_date + "," + end_date

    config["defaults"]["source_options"]["range"] = range_date

    return config