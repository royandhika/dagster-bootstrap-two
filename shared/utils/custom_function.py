from dagster import AssetSpec, AssetKey, EnvVar
from dagster._core.definitions.time_window_partitions import TimeWindow
from datetime import datetime
from pandas import DataFrame
from sqlalchemy import create_engine
from typing import Any, Literal
import yaml
import os
import json


def external_asset(kind: set[str], group:str, tables: list) -> list[AssetSpec]:
    return [
        AssetSpec(
            key=AssetKey(["sources", table]),
            # description=table["desc"],
            group_name=group,
            kinds=kind
        )
        for table in tables
    ]


def sling_yaml_dict(filename: str) -> dict[str, Any]:
    env = EnvVar("ENV_SCHEMA").get_value() or ""

    def inject_env(obj: Any) -> Any:
        if isinstance(obj, dict):
            return {k: inject_env(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [inject_env(v) for v in obj]
        elif isinstance(obj, str):
            return obj.replace("{ENV_SCHEMA}", env)
        return obj
    
    sling_dir = os.path.join(os.path.dirname(__file__), "../../../app/sling")
    path = os.path.abspath(os.path.join(sling_dir, filename))
            
    with open(path, "r") as file:
        config = yaml.safe_load(file)

    return inject_env(config)


def sling_add_backfill(config: dict, time_window: TimeWindow) -> dict:
    start, end = time_window

    range_date = f"{datetime.strftime(start, '%Y-%m-%d %H:%M')},{datetime.strftime(end, '%Y-%m-%d %H:%M')}"
    
    config["defaults"]["source_options"]["range"] = range_date

    return config


def pandas_write_table(table_name: str, method: Literal["fail", "replace", "append"], data: DataFrame):
    conn_string = EnvVar("ITSQL_STRING").get_value() or ""
    engine = create_engine(conn_string)
    schema = EnvVar("ENV_SCHEMA").get_value() or ""
    data["uploaddate"] = datetime.now()
    
    for col in data.columns:
        if data[col].apply(lambda x: isinstance(x, (dict, list, set, tuple))).any():
            data[col] = data[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list, set, tuple)) else x)
    
    data.to_sql(
        schema=schema+'_dl', 
        name=table_name, 
        con=engine, 
        if_exists=method, 
        index=False
    )