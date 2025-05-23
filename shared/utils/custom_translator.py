from dagster import AssetKey, AssetSpec, AssetDep, Config
from dagster_dbt import DagsterDbtTranslator
from dagster_sling import DagsterSlingTranslator
from typing import Mapping, Optional, Any, Literal


class CustomDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        # resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        schema = dbt_resource_props["schema"]
        # fqn = dbt_resource_props["fqn"][1]

        if "_dl" in schema:
            prefix = "landings"
        elif "_di" in schema:
            prefix = "integrations"
        elif "_dm" in schema:
            prefix = "marts" 
        else:
            prefix = "default"

        return AssetKey([prefix, name])
    

class CustomSlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition) -> AssetSpec:
        asset_spec = super().get_asset_spec(stream_definition)

        original_deps = asset_spec.deps

        new_asset_spec = asset_spec.replace_attributes(
            key=AssetKey(["landings"] + list(asset_spec.key.path)),
            kinds={"sqlserver", "sling"},
            deps=[
                AssetDep(
                    asset=AssetKey(["sources"] + list(dep.asset_key.path)),
                    partition_mapping=dep.partition_mapping,
                )
                for dep in original_deps
            ]
        )
        return new_asset_spec
    
    def _default_description_fn(self, stream_definition: Mapping[str, Any]) -> Optional[str]:
        config = stream_definition.get("config", {})
        meta = config.get("meta", {})
        return meta.get("dagster", {}).get("description")
    

class CustomDbtRun(Config):
    full_refresh: bool = False
    threads: int = 2


class CustomPandasRun(Config):
    method: Literal["fail", "replace", "append"] = "append"
    full_refresh: bool = False