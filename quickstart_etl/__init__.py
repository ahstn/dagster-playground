from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)
from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from .assets import pagila
from .assets.pagila.dbt import dbt_resource as pagila_dbt
from .resources.infra import PagilaDatabase
from .resources.trino import TrinoDatabase
from .trino_io_manager.resources import build_fsspec_resource
from .trino_io_manager import build_trino_iomanager
from .trino_io_manager.type_handlers import PandasArrowTypeHandler
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

trino_io_manager = build_trino_iomanager([
    PandasArrowTypeHandler()
])

defs = Definitions(
    assets=[*pagila_assets], 
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
        "trino_io_manager": trino_io_manager.configured({
            "catalog": "hive",
            "schema": "public",
            "user": "trino",
            "host": "localhost",
            "port": 8080,
            "connector": "trino",
        }),
        "fsspec": build_fsspec_resource({
            "protocol": "s3",
        }).configured({
            "tmp_path": "./output",
        })
    }
)
