from dagster import FreshnessPolicy, asset
from typing import Any, Mapping
import pandas as pd
from ...resources.infra import PagilaDatabase

"""
Assets in here use the DuckDB IO Manager to delegate responsibility
for creating and updating tables.

see: https://docs.dagster.io/integrations/duckdb/using-duckdb-with-dagster
"""

@asset(
    description="The customer table from the Pagila database",
    compute_kind="SQL",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60)
)
def customer(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM customer;", psql_conn.connection())

@asset(
    compute_kind="SQL",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60)
)
def staff(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM staff;", psql_conn.connection())

@asset(
    compute_kind="SQL",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60)
)
def film(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM film;", psql_conn.connection())

@asset(
    compute_kind="SQL",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60)
)
def category(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM category;", psql_conn.connection())

@asset(
    compute_kind="SQL",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60)
)
def film_category(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM film_category;", psql_conn.connection())

@asset(
    compute_kind="SQL",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60)
)
def inventory(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM inventory;", psql_conn.connection())

@asset(
    compute_kind="SQL",
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=60)
)
def rental(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM rental;", psql_conn.connection())
