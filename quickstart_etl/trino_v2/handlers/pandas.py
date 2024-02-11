from dagster import InputContext, OutputContext
from dagster._core.storage.db_io_manager import TableSlice
from pyarrow import parquet
from trino.exceptions import TrinoQueryError
import pandas as pd
import pyarrow
import trino

from ..utils import arrow as arrow_utils
from ..io_manager import TrinoDbClient
from ..types import TableFilePaths, TrinoQuery
from .base import TrinoBaseTypeHandler
from .arrows import ArrowTypeHandler


class PandasTypeHandler(TrinoBaseTypeHandler):
    """Stores and loads Pandas DataFrames in Trino accessing the underlying parquet files
    through the object storage or file system backing a Trino Hive catalog.
    To use this type handler, pass it to ``build_trino_io_manager``.

    The `PandasArrowTypeHandler` requires an `fsspec` resource to be set up 
    in order to access the storage layer. 

    Example:
        .. code-block:: python
            from dagster_trino.io_manager import build_trino_io_manager
            from dagster_trino.resources import build_fsspec_resource
            from dagster_trino.type_handlers import PandasArrowTypeHandler
            from dagster import Definitions
            import pandas as pd
            
            @asset(io_manager_key='trino_io_manager')
            def my_table() -> pd.DataFrame
                ...
            fsspec_params = {...} #dict containing fsspec storage options
            fsspec_resource = dagster_trino.resources.build_fsspec_resource(fsspec_params)
            trino_io_manager = build_trino_io_manager([PandasArrowTypeHandler()])
            
            defs = Definitions(
                assets=[my_table],
                resources={
                    "trino_io_manager": trinoquery_io_manager.configured({...}),
                    "fsspec": fsspec_resource.configured({...})
                }
            )
    """
    def __init__(self):
        self.arrow_handler = ArrowTypeHandler()

    def handle_output(self, context: OutputContext, table_slice: TableSlice, obj: pd.DataFrame, connection: trino.dbapi.Connection):
        """Loads content of files saved at the given location into a Trino managed table."""
        if len(obj) == 0:
            raise FileNotFoundError("The list of files to load in the table is empty.")

        try:
            obj.to_sql(
                f"{table_slice.schema}.{table_slice.table}",
                con=connection.cursor(),
                if_exists="append",
                index=False
            )
            # connection.execute(
            #     f'''
            #     CREATE TABLE {table_slice.schema}.{table_slice.table}
            #     WITH (format = 'PARQUET') 
            #     AS (SELECT * FROM obj)
            #     '''
            # )
        except TrinoQueryError as e:
            if e.error_name != 'TABLE_ALREADY_EXISTS':
                raise e
            
            connection.execute(
                f"INSERT INTO {table_slice.schema}.{table_slice.table} (SELECT * FROM obj)"
            )
        context.add_output_metadata(
            {
                "data_frame": obj,
                "tmp_query": create_query
            }
        )
    
    def load_input(self, context: InputContext, table_slice: TableSlice, connection):
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pd.DataFrame()
        
        return pd.DataFrame(connection.execute("SELECT * FROM {table_slice.schema}.{table_slice.table}").fetchall())
    
    @property
    def supported_types(self):
        return [pd.DataFrame]
    
    @property
    def requires_fsspec(self):
        return False