from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from .assets import pagila
from .assets.iris_csv import iris_cleaned, iris_dataset
from .assets.pagila.dbt import dbt_resource as pagila_dbt
from .resources.infra import PagilaDatabase
from .resources.trino import TrinoDatabase
from .trino_io_manager import TrinoIOManager
from .assets.pagila.trino import load_trino

pagila_assets = load_assets_from_package_module(
    pagila,
    group_name="pagila",
    # all of these assets live in the duckdb database, under the schema raw_data
    key_prefix=["pagila"],
)

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)

defs = Definitions(
    assets=[*pagila_assets, iris_cleaned, iris_dataset], 
    schedules=[daily_refresh_schedule],
    jobs=[load_trino],
    resources={
        "psql_conn": PagilaDatabase(),
        "trino_conn": TrinoDatabase(),
        "duckdb": DuckDBResource(
            database="database.duckdb", 
            schema="public",
        ),
        "duckdb_io": DuckDBPandasIOManager(
            database="database.duckdb",
            schema="public",
        ),
        "dbt": pagila_dbt,
        "trino_io_manager": TrinoIOManager(
            bucket="warehouse",
            connection_config={
                "catalog": "hive",
                "schema": "test",
                "user": "trino",
                "host": "localhost",
                "port": 8080,
            },
            type_handler="parquet",
            fs_config={
                "protocol": "s3a",
                "key": "minio",
                "secret": "minio123",
                "endpoint_url": "http://localhost:4566"
            },
        )
        # "trino_io_manager": build_trino_io_manager([ParquetTypeHandler()]).configured({
        #     "catalog": "hive",
        #     "schema": "test",
        #     "user": "trino",
        #     "host": "localhost",
        #     "port": 8080,
        #     "bucket": "warehouse",
        # }),
        # "fsspec": build_fsspec_resource({
        #     "protocol": "s3a",
        #     # https://s3fs.readthedocs.io/en/latest/api.html#s3fs.core.S3FileSystem
        #     "key": "minio",
        #     "secret": "minio123",
        #     "endpoint_url": "http://localhost:9000",
        # }).configured({
        #     # For S3, this is `{bucket}/{optional_path}`
        #     "tmp_path": "warehouse",
        # }),
    }
)
