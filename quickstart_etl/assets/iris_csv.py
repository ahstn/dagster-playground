from dagster import asset
from dagster_duckdb import DuckDBResource
import pandas as pd

@asset(
    io_manager_key="delta_io",
)
def iris_dataset() -> pd.DataFrame:
    return pd.read_csv(
        "https://docs.dagster.io/assets/iris.csv",
        names=[
            "sepal_length_cm",
            "sepal_width_cm",
            "petal_length_cm",
            "petal_width_cm",
            "species",
        ],
    )

@asset(
    io_manager_key="delta_io",
)
def iris_cleaned(iris_dataset: pd.DataFrame) -> pd.DataFrame:
    return iris_dataset.dropna().drop_duplicates()
