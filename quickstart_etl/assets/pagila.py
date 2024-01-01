from dagster import job, op, asset
import pandas as pd
from ..resources.infra import postgres_connection, PagilaDatabase

@asset(group_name="pagila", compute_kind="Pagila DB")
def customer(psql_conn: PagilaDatabase) -> pd.DataFrame:
    return pd.read_sql("SELECT * FROM customer", psql_conn.connection())
