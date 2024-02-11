from dagster import InputContext, OutputContext
from dagster._core.storage.db_io_manager import TableSlice
from pyarrow import parquet
import pandas as pd
import pyarrow

from ..utils import arrow as arrow_utils
from ..io_manager import TrinoDbClient
from ..types import TableFilePaths, TrinoQuery
from .base import TrinoBaseTypeHandler

import os

class ArrowTypeHandler(TrinoBaseTypeHandler):
    """Stores and loads Arrow Tables in Trino accessing the underlying parquet files
    through the object storage or file system backing a Trino Hive catalog.
    To use this type handler, pass it to ``build_trino_io_manager``.

    The `ArrowTypeHandler` requires an `fsspec` resource to be set up 
    in order to access the storage layer. 

    Example:
        .. code-block:: python
            from dagster_trino.io_manager import build_trino_io_manager
            from dagster_trino.resources import build_fsspec_resource
            from dagster_trino.type_handlers import ArrowTypeHandler
            from dagster import Definitions
            import pyarrow as pa
            
            @asset(io_manager_key='trino_io_manager')
            def my_table() -> pa.Table
                ...
            fsspec_params = {...} #dict containing fsspec storage options
            fsspec_resource = dagster_trino.resources.build_fsspec_resource(fsspec_params)
            trino_io_manager = build_trino_io_manager([ArrowTypeHandler()])
            
            defs = Definitions(
                assets=[my_table],
                resources={
                    "trino_io_manager": trinoquery_io_manager.configured({...}),
                    "fsspec": fsspec_resource.configured({...})
                }
            )
    """

    def handle_output(
            self, context: OutputContext, table_slice: TableSlice, obj: pyarrow.Table, connection
        ):
        """Loads content of files saved at the given location into a Trino managed table."""
        if len(obj) == 0:
            raise FileNotFoundError("The list of files to load in the table is empty.")

        drop_query = f'''
            DROP TABLE IF EXISTS {tmp_table_name}
        '''
        create_query = f'''
            CREATE TABLE {tmp_table_name}(
                {trino_columns}
            )
            WITH (
                format = 'PARQUET',
                external_location = '{table_dir}'
            )
        '''
        select_query = f'''
            SELECT * FROM {tmp_table_name}
        '''
        connection.execute(f"{drop_query}") #if previous cleanup failed
        connection.execute(f"{create_query}")
        try:
            connection.execute(
                f'''
                CREATE TABLE {table_slice.schema}.{table_slice.table}
                WITH (format = 'PARQUET') 
                AS ({select_query})
                '''
            )
        except TrinoQueryError as e:
            if e.error_name != 'TABLE_ALREADY_EXISTS':
                raise e
            # table was not created, therefore already exists. Insert the data
            connection.execute(
                f"insert into {table_slice.schema}.{table_slice.table} ({select_query})"
            )
        context.add_output_metadata(
            {
                "file_paths": obj,
                "tmp_query": create_query
            }
        )
        connection.execute(f"{drop_query}") #cleanup temp table

    def load_input(self, context: InputContext, table_slice: TableSlice, connection) -> pyarrow.Table:
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pyarrow.Table()
        file_paths = self.file_handler.load_input(context, table_slice, connection)

        with context.resources.fsspec.get_fs() as fs:
            arrow_df = parquet.ParquetDataset(file_paths, filesystem=fs)
        return arrow_df.read()
    
    @property
    def supported_types(self):
        return [pyarrow.Table]
    
    @property
    def requires_fsspec(self):
        return True