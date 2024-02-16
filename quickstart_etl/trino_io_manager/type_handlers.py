from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from dagster import InputContext, OutputContext
from .io_manager import TrinoDbClient
from .types import TableFilePaths, TrinoQuery

from typing import List

from trino.exceptions import TrinoQueryError

import pandas as pd
import pyarrow
from pyarrow import parquet
from .utils import arrow as arrow_utils

import os

    
class ArrowTypeHandler(DbTypeHandler):
    def upload_files(self, context: OutputContext, table_slice: TableSlice, obj: pyarrow.Table) -> str:
        """
        Using fsspec, upload the parquet files to the storage layer
        """
        with context.resources.fsspec.get_fs() as fs:
            file_path = os.path.join(
                context.resources.fsspec.tmp_folder, 
                f"{table_slice.schema}_{table_slice.table}/"
                f"{table_slice.schema}_{table_slice.table}.parquet")
            fs.makedirs(os.path.dirname(file_path), exist_ok=True)

            parquet.write_table(obj, file_path, filesystem=fs)
            # TODO: Delete files after

            return os.path.dirname(file_path)
        

    def create_table(self, context: OutputContext, table_slice: TableSlice, table_dir: str, obj: pyarrow.table, connection):
        """
        Create a table in Trino using the schema of the Arrow Table.
        NB: Sometimes a temporary table is necessary to load data into the destination (if pd can't produce trino SQL values)
        """
        arrow_schema = obj.schema
        trino_columns = arrow_utils._get_trino_columns_from_arrow_schema(arrow_schema)
        table = f'{table_slice.schema}.{table_slice.table}'
        context.log.info(arrow_schema)
        context.log.info(trino_columns)
        
        drop_query = f'DROP TABLE IF EXISTS {table}'
        create_query = f'''
            CREATE TABLE {table}(
                {trino_columns}
            )
            WITH (
                format = 'PARQUET',
                external_location = 's3a://{table_dir}'
            )
        '''
        connection.execute(drop_query)
        connection.execute(create_query)

        context.add_output_metadata({
            "table_directory": table_dir,
            "create_query": create_query,
            "table": table
        })

    # def handle_output(self, context: OutputContext, table_slice: TableSlice, obj: Union[pd.DataFrame, pyarrow.Table], connection):
    #     print("Handling output")

    #     if isinstance(obj, PandasDataFrame):
    #         print("Handling output PD dataframe")

    def handle_output(self, context: OutputContext, table_slice: TableSlice, obj: pyarrow.Table, connection):
        table_dir = self.upload_files(context, table_slice, obj)
    
        try:
            self.create_table(context, table_slice, table_dir, obj, connection)
        except Exception as e:
            context.log.error(f"Error while loading the parquet files into Trino: {e}")
            raise e

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
    
class PandasArrowTypeHandler(DbTypeHandler):
    def __init__(self):
        self.arrow_handler = ArrowTypeHandler()

    def handle_output(self, context: OutputContext, table_slice: TableSlice, obj: pd.DataFrame, connection):
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pd.DataFrame()
        return self.arrow_handler.handle_output(context, table_slice, pyarrow.Table.from_pandas(obj), connection)
    
    def load_input(self, context: InputContext, table_slice: TableSlice, connection):
        return self.arrow_handler.load_input(context, table_slice, connection).to_pandas()
    
    @property
    def supported_types(self):
        return [pd.DataFrame]
    
    @property
    def requires_fsspec(self):
        return True