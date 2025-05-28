from dagster import EnvVar
from dagster._core.definitions.time_window_partitions import TimeWindow
from datetime import datetime, timedelta
from pandas import DataFrame
from sqlalchemy import create_engine
from typing import Any, Literal, Sequence
from io import StringIO
from pathlib import Path
import pandas as pd
import yaml
import os
import json


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


def sling_add_backfill(yaml: dict, time_window: TimeWindow) -> dict:
    start, end = time_window
    start -= timedelta(minutes=30)
    range_date = f"{datetime.strftime(start, '%Y-%m-%d %H:%M')},{datetime.strftime(end, '%Y-%m-%d %H:%M')}"
    
    yaml["defaults"]["source_options"]["range"] = range_date

    return yaml


def pandas_write_table(table_name: str, method: Literal["fail", "replace", "append"], data: DataFrame):
    conn_string = EnvVar("ITSQL_STRING").get_value() or ""
    engine = create_engine(conn_string, fast_executemany=True)
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
        index=False,
        chunksize=50000
    )


def sanitize_csv(path: Path) -> StringIO:
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    # Get expected number of columns
    expected_num_columns = lines[0].count('|') + 1

    cleaned_lines = []
    current_line = ""
    rn = 0

    for line in lines:
        rn += 1 # Line counter
        stripped_line = line.strip()

        if not stripped_line:
            continue

        if current_line:
            current_line += " " + stripped_line
        else:
            current_line = stripped_line

        # Count the number of '|' in the current line
        num_pipes = current_line.count('|')

        if num_pipes == expected_num_columns - 1:
            cleaned_lines.append(current_line)
            current_line = ""
        if num_pipes > expected_num_columns - 1:
            raise ValueError(f"Extra '|' detected in line {rn}: {line})")
        
    if current_line:
        cleaned_lines.append(current_line)

    cleaned_content = "\n".join(cleaned_lines)

    return StringIO(cleaned_content)


def get_file_path(base_path: str, folder: Sequence[str], pattern: str) -> Path:
    search_path = Path(base_path, *folder)
    files = sorted(search_path.glob(pattern))
    if files:
        return files[0]
    else:
        raise ValueError(f"File {pattern} not found")


def read_file_csv(data, sep: str, dtype: str) -> DataFrame:
    return pd.read_csv(data, sep=sep, dtype=dtype).convert_dtypes()