from dagster import (
    Definitions,
    ScheduleDefinition,
    define_asset_job,
    load_assets_from_package_module,
)

from dagster_duckdb import DuckDBResource
from dagster_duckdb_pandas import DuckDBPandasIOManager

from . import assets
from .assets.pagila import (dbt_resource)
from .resources.infra import PagilaDatabase

daily_refresh_schedule = ScheduleDefinition(
    job=define_asset_job(name="all_assets_job"), cron_schedule="0 0 * * *"
)

defs = Definitions(
    assets=load_assets_from_package_module(assets), 
    schedules=[daily_refresh_schedule],
    resources={
        "psql_conn": PagilaDatabase(),
        "duckdb": DuckDBResource(
            database="database.duckdb", 
            schema="public",
        ),
        "duckdb_io": DuckDBPandasIOManager(
            database="database.duckdb",
            schema="public",
        ),
        "dbt": dbt_resource,
    }
)
