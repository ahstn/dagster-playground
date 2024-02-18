from dagster import ConfigurableIOManagerFactory, Field, IOManagerDefinition, io_manager
from dagster import Bool, Field, IntSource, StringSource, Shape, Permissive
from dagster._core.storage.db_io_manager import DbIOManager, DbTypeHandler
from typing import  Optional, Sequence, Type, Any, Dict, TypedDict

from .configs import define_trino_config
from .db_client import TrinoDbClient
from .handlers.delta_lake import DeltaLakeHandler
from .handlers.parquet import ParquetTypeHandler


def build_trino_io_manager(
    type_handlers: Sequence[DbTypeHandler], default_load_type: Optional[Type] = None
) -> IOManagerDefinition:
    """
    Builds an IO manager definition that reads inputs from and writes outputs to Trino. 
    Args:
        type_handlers (Sequence[DbTypeHandler]): Each handler will execute Trino Queries or translate between
            Trino tables and an in-memory type - e.g. a Pandas DataFrame. If only
            one DbTypeHandler is provided, it will be used as the default_load_type.
        default_load_type (Type): When an input has no type annotation, load it as this type.
    Returns:
        IOManagerDefinition
    """
    required_resource_keys = set()
    if any(type_handler.requires_fsspec for type_handler in type_handlers):
        required_resource_keys.add('fsspec')

    @io_manager(
        config_schema=define_trino_config(),
        required_resource_keys=required_resource_keys
    )
    def trino_io_manager(init_context):
        return DbIOManager(
            type_handlers=type_handlers,
            db_client=TrinoDbClient(),
            io_manager_name="TrinoIoManager",
            database=init_context.resource_config["catalog"],
            schema=init_context.resource_config.get("schema"),
            default_load_type=default_load_type,
        )
    
    return trino_io_manager


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
        return [ParquetTypeHandler()]

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
