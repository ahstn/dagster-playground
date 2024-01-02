from dagster import asset
from dagster_duckdb import DuckDBResource
import pandas as pd

@asset
def iris_dataset(duckdb: DuckDBResource) -> None:
    iris_df = pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )
    with duckdb.get_connection() as conn:
        conn.execute("CREATE TABLE iris_dataset AS SELECT * FROM iris_df")

@asset(deps=[iris_dataset])
def iris_setosa(duckdb: DuckDBResource) -> None:
    with duckdb.get_connection() as conn:
        conn.execute(
            "CREATE TABLE iris_setosa AS SELECT * FROM iris_dataset WHERE species = 'Iris-setosa'"
        )
