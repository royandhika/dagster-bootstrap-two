from dagster import AssetSelection, define_asset_job


outbound_mrsdso_dbt = AssetSelection.groups("outbound_mrsdso") - AssetSelection.key_prefixes(["landings"])

job_outbound_mrsdso = define_asset_job(
    name="job_outbound_mrsdso",
    selection=outbound_mrsdso_dbt
)