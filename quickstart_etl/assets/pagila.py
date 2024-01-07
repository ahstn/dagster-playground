from dagster import AssetExecutionContext, AssetKey, file_relative_path, asset
from dagster_dbt import (
    DagsterDbtTranslator,
    DbtCliResource,
    dbt_assets,
    get_asset_key_for_model,
)
from typing import Any, Mapping
import pandas as pd
from ..resources.infra import PagilaDatabase

"""
Assets in here use the DuckDB IO Manager to delegate responsibility
for creating and updating tables.

see: https://docs.dagster.io/integrations/duckdb/using-duckdb-with-dagster
"""

@asset(group_name="pagila", compute_kind="Pagila DB")
def customer(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM customer;", psql_conn.connection())

@asset(group_name="pagila", compute_kind="Pagila DB")
def staff(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM staff;", psql_conn.connection())

@asset(group_name="pagila", compute_kind="Pagila DB")
def film(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM film;", psql_conn.connection())

@asset(group_name="pagila", compute_kind="Pagila DB")
def category(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM category;", psql_conn.connection())

@asset(group_name="pagila", compute_kind="Pagila DB")
def film_category(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM film_category;", psql_conn.connection())

@asset(group_name="pagila", compute_kind="Pagila DB")
def inventory(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM inventory;", psql_conn.connection())

@asset(group_name="pagila", compute_kind="Pagila DB")
def rental(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM rental;", psql_conn.connection())

DBT_PROJECT_DIR = file_relative_path(__file__, "../../pagila_dbt")

dbt_resource = DbtCliResource(project_dir=DBT_PROJECT_DIR)
dbt_parse_invocation = dbt_resource.cli(["parse"]).wait()
dbt_manifest_path = dbt_parse_invocation.target_path.joinpath("manifest.json")


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        asset_key = super().get_asset_key(dbt_resource_props)

        if dbt_resource_props["resource_type"] == "model":
            asset_key = asset_key.with_prefix(["duckdb", "dbt_schema"])
        if dbt_resource_props["resource_type"] == "source":
            asset_key = asset_key.with_prefix("duckdb")

        return asset_key


@dbt_assets(
    manifest=dbt_manifest_path,
    dagster_dbt_translator=CustomDagsterDbtTranslator(),
)
def dbt_project_assets(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


# daily_order_summary_asset_key = get_asset_key_for_model([dbt_project_assets], "daily_order_summary")