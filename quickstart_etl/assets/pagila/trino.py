import time

from dagster import op, graph, job
from pandas import DataFrame

from ...resources.trino import TrinoDatabase
from .source_data import customer, staff, film, category, film_category, inventory, rental



@op
def load_data(context, transformed_data: DataFrame, trino_conn: TrinoDatabase):
    transformed_data.to_sql('engineers', trino_conn.connection(), if_exists='replace', index=False)
    context.log.info('Loaded transformed data into the database')

@job
def load_trino():
    load_data(customer())