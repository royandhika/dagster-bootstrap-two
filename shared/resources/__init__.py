from pathlib import Path
from dagster import EnvVar
from dagster_dbt import DbtProject, DbtCliResource
from dagster_sling import SlingConnectionResource, SlingResource
from typing import Tuple


sling_itsql_conn = SlingConnectionResource(
    name="itsql",
    type=EnvVar("ITSQL_TYPE"),
    host=EnvVar("ITSQL_HOST"),
    database=EnvVar("ITSQL_DB"),
    port=EnvVar("ITSQL_PORT"),
    user=EnvVar("ITSQL_USER"),
    password=EnvVar("ITSQL_PASS"),
    trust_server_certificate="true",
)

sling_slave_conn = SlingConnectionResource(
    name="slave",
    type=EnvVar("SLAVE_TYPE"),
    host=EnvVar("SLAVE_HOST"),
    database=EnvVar("SLAVE_DB"),
    port=EnvVar("SLAVE_PORT"),
    user=EnvVar("SLAVE_USER"),
    password=EnvVar("SLAVE_PASS"),
)

resource_sling = SlingResource(connections=[sling_itsql_conn, sling_slave_conn])

def create_dbt_project(profile_name: str = "awodb") -> Tuple[DbtProject, DbtCliResource]:
    base_path = Path(__file__).joinpath("..", "..", "..", "dbt").resolve() #local dagster dev
    dbt_path = DbtProject(
        project_dir=base_path,
        profiles_dir=base_path,
        profile=profile_name,
    )
    dbt_path.prepare_if_dev()

    dbt_resource = DbtCliResource(project_dir=dbt_path)

    return dbt_path, dbt_resource

path_dbt, resource_dbt = create_dbt_project()