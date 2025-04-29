from pathlib import Path
from dagster import EnvVar, ConfigurableResource
from dagster_dbt import DbtProject, DbtCliResource
from dagster_sling import SlingConnectionResource, SlingResource
from typing import Tuple, Optional, Literal
from shared.resources.api_config import API_LIST_PROJECTS
from shared.utils.custom_function import pandas_write_table
from pandas import DataFrame
from requests import post, auth, request


sling_itsql_conn = SlingConnectionResource(
    name="itsql",
    type=EnvVar("ITSQL_TYPE"),
    host=EnvVar("ITSQL_HOST"), # type: ignore
    database=EnvVar("ITSQL_DB"), # type: ignore
    port=EnvVar("ITSQL_PORT"), # type: ignore
    user=EnvVar("ITSQL_USER"), # type: ignore
    password=EnvVar("ITSQL_PASS"), # type: ignore
    trust_server_certificate="true", # type: ignore
)

sling_slave_conn = SlingConnectionResource(
    name="slave",
    type=EnvVar("SLAVE_TYPE"),
    host=EnvVar("SLAVE_HOST"), # type: ignore
    database=EnvVar("SLAVE_DB"), # type: ignore
    port=EnvVar("SLAVE_PORT"), # type: ignore
    user=EnvVar("SLAVE_USER"), # type: ignore
    password=EnvVar("SLAVE_PASS"), # type: ignore
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


class APIResource(ConfigurableResource):
    def request(
        self, 
        project: str, 
        url: str, 
        table_name: str, 
        pandas_method: Literal["fail", "replace", "append"], 
        payload: Optional[dict], 
        http_method: str = "GET"
    ) -> DataFrame:
        login_attr = API_LIST_PROJECTS.get(project)
        assert login_attr is not None, f"No project {project}"

        res = post(
            url=login_attr["login_endpoint"],
            auth=auth.HTTPBasicAuth(login_attr["username"], login_attr["password"])
        )
        res.raise_for_status()
        token = res.json()["access_token"]["token"]

        headers = {"Authorization": f"Bearer {token}"}
        res = request(
            method=http_method, 
            url=url, 
            headers=headers, 
            json=payload
        )
        res.raise_for_status()

        data = DataFrame(res.json())
        pandas_write_table(
            table_name=table_name,
            method=pandas_method,
            data=data
        )

        return data

resource_api = APIResource()