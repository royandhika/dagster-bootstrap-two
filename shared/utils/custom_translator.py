from dagster import AssetKey, AssetSpec, AssetDep
from dagster_dbt import DagsterDbtTranslator
from dagster_sling import DagsterSlingTranslator

class CustomDbtTranslator(DagsterDbtTranslator):
    def get_asset_key(self, dbt_resource_props) -> AssetKey:
        # resource_type = dbt_resource_props["resource_type"]
        name = dbt_resource_props["name"]
        schema = dbt_resource_props["schema"]
        # fqn = dbt_resource_props["fqn"][1]

        if schema in ("eth_dl", "awo_dl"):
            prefix = "landings"
        elif schema in ("eth_di", "awo_di"):
            prefix = "integrations"
        elif schema in ("eth_dm", "awo_dm"):
            prefix = "marts" 
        else:
            prefix = "default"

        return AssetKey([prefix, name])
    
class CustomSlingTranslator(DagsterSlingTranslator):
    def get_asset_spec(self, stream_definition) -> AssetSpec:
        asset_spec = super().get_asset_spec(stream_definition)

        original_deps = asset_spec.deps

        new_asset_spec = asset_spec.replace_attributes(
            key=AssetKey(["landings"] + asset_spec.key.path),
            kinds={"sqlserver", "sling"},
            # description=,
            deps=[
                AssetDep(
                    asset=AssetKey(["sources"] + dep.asset_key.path),
                    partition_mapping=dep.partition_mapping,
                )
                for dep in original_deps
            ]
        )
        return new_asset_spec