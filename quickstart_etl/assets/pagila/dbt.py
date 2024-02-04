from dagster import AssetExecutionContext
from dagster_dbt import (
    DbtCliResource,
    dbt_assets,
)
from os import path, getcwd

"""
Assets in here are dynamically generated from the DBT project. 
This is done by parsing `manifest.json` in the DBT project, thus is unique per project.

If you have multiple DBT projects, you'll likely repeat this code for each.

see: https://docs.dagster.io/integrations/dbt
"""

DBT_PROJECT_DIR = path.join(getcwd(), "pagila_dbt")

dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)
dbt_parse_invocation = dbt_resource.cli(["parse"]).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")

@dbt_assets(manifest=dbt_manifest_path)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()