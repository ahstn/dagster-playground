from dagster import IOManagerDefinition, io_manager
from dagster._core.storage.db_io_manager import (
    DbIOManager,
    DbTypeHandler,
)
from typing import Optional, Sequence, Type

from .configs import define_trino_config
from .db_client import TrinoDbClient


def build_trino_iomanager(
    type_handlers: Sequence[DbTypeHandler], default_load_type: Optional[Type] = None
) -> IOManagerDefinition:
    """Builds an IO manager definition that reads inputs from and writes outputs to Trino. 
    Args:
        type_handlers (Sequence[DbTypeHandler]): Each handler will execute Trino Queries or translate between
            Trino tables and an in-memory type - e.g. a Pandas DataFrame. If only
            one DbTypeHandler is provided, it will be used as the default_load_type.
        default_load_type (Type): When an input has no type annotation, load it as this type.
    Returns:
        IOManagerDefinition
    Examples:
        .. code-block:: python
            from dagster_trino import build_trino_io_manager
            from dagster_trino.type_handlers import ArrowPandasTypeHandler
            @asset(
                key_prefix=["my_schema"]  # will be used as the schema in Trino
            )
            def my_table() -> pd.DataFrame:  # the name of the asset will be the table name
                ...
            trino_io_manager = build_trino_io_manager([ArrowPandasTypeHandler()])
            @repository
            def my_repo():
                return with_resources(
                    [my_table],
                    {"io_manager": trino_io_manager.configured({...})}
                )
    If you do not provide a schema in your configuration, Dagster will determine a schema based on the assets and ops using
    the IO Manager. For assets, the schema will be determined from the asset key. For ops, the schema can be
    specified by including a "schema" entry in output metadata. If none of these is provided, the schema will
    default to "public".
    .. code-block:: python
        @op(
            out={"my_table": Out(metadata={"schema": "my_schema"})}
        )
        def make_my_table() -> pd.DataFrame:
            ...
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
