from dagster import ConfigurableIOManagerFactory, Field, IOManagerDefinition, io_manager
from dagster import Bool, Field, IntSource, StringSource, Shape, Permissive
from dagster._core.storage.db_io_manager import DbIOManager, DbTypeHandler
from typing import  Optional, Sequence, Type, Any, Dict, TypedDict

from .configs import define_trino_config
from .db_client import TrinoDbClient
from .handlers.delta_lake import DeltaLakeHandler
from .handlers.parquet import ParquetTypeHandler


class TrinoIOManager(ConfigurableIOManagerFactory):
    """Base class for an IO manager definition that reads inputs from and writes outputs to DuckDB.
    """

    # TrinoConfigType = TypedDict("TrinoConfigType", {
    #     "catalog": str,
    #     "schema": str,
    #     "user": str,
    #     "host": str,
    #     "port": int,
    #     "password": Optional[str],
    # })
    

    # dagster.Field incorrectly throws: TypeError: Shape.__new__() missing 1 required positional argument: 'fields'
    # Trino SQL Alchemy configuration, see: https://github.com/trinodb/trino-python-client
    connection_config: Dict[str, Any] # type: ignore[assignment]

    # FSSpec configuration, see: https://s3fs.readthedocs.io/en/latest/api.html"
    fs_config: Dict[str, Any]

    type_handler: str

    bucket: str

    @staticmethod
    def type_handlers() -> Sequence[DbTypeHandler]:
        return [DeltaLakeHandler()]

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return None

    def create_io_manager(self, context) -> DbIOManager:
        return DbIOManager(
            db_client=TrinoDbClient(),
            database=str(self.connection_config.get("catalog")),
            schema=str(self.connection_config.get("schema")),
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
            io_manager_name="TrinoIOManager",
        )
