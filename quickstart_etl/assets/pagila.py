from dagster import asset
from dagster_duckdb import DuckDBResource
import pandas as pd
from ..resources.infra import PagilaDatabase

@asset(group_name="pagila", compute_kind="Pagila DB")
def customer(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM customer;", psql_conn.connection())

@asset(group_name="pagila", compute_kind="Pagila DB")
def film(psql_conn: PagilaDatabase, duckdb: DuckDBResource) -> None:
    film_df =  pd.read_sql("SELECT * FROM film;", psql_conn.connection())
    with duckdb.get_connection() as conn:
        conn.execute("CREATE TABLE film AS SELECT * FROM film_df")
