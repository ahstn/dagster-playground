from dagster import asset
from dagster_duckdb import DuckDBResource
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

