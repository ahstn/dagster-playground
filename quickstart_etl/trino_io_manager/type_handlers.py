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

class TrinoBaseTypeHandler(DbTypeHandler):
    """
    Base Type Handler Class. Should not be called directly, but 
    rather used through one of its sub-classes.
    """
    @property
    def requires_fsspec(self):
        False
    
class FilePathTypeHandler(TrinoBaseTypeHandler):
    """Stores and loads Parquet Data in Trino 
    through the object storage or file system backing a Trino Hive catalog.
    To use this type handler, pass it to ``build_trino_io_manager``.

    The `FilePathTypeHandler requires  an `fsspec` resource to be set up 
    in order to access the storage layer. 

    Example:
        .. code-block:: python
            from dagster_trino.io_manager import build_trino_io_manager
            from dagster_trino.resources import build_fsspec_resource
            from dagster_trino.type_handlers import FilePathTypeHandler
            from dagster_trino.types import TableFilePaths
            from dagster import Definitions
            
            @asset(io_manager_key='trino_io_manager')
            def my_table() -> TableFilePaths
                ...
            fsspec_params = {...} #dict containing fsspec storage options
            fsspec_resource = dagster_trino.resources.build_fsspec_resource(fsspec_params)
            trino_io_manager = build_trino_io_manager([FilePathTypehandler()])
            
            defs = Definitions(
                assets=[my_table],
                resources={
                    "trino_io_manager": trinoquery_io_manager.configured({...}),
                    "fsspec": fsspec_resource.configured({...})
                }
            )
    """
    def handle_output(
        self, context: OutputContext, table_slice: TableSlice, obj: TableFilePaths, connection
    ):
        """Loads content of files saved at the given location into a Trino managed table."""
        if len(obj) == 0:
            raise FileNotFoundError("The list of files to load in the table is empty.")
        table_dir = os.path.dirname(obj[0])
        
        with context.resources.fsspec.get_fs() as fs:
            arrow_schema = parquet.read_schema(obj[0], filesystem=fs)
            trino_columns = arrow_utils._get_trino_columns_from_arrow_schema(arrow_schema)
        context.log.info(arrow_schema)
        context.log.info(trino_columns)
        tmp_table_name = f'{table_slice.schema}.tmp_dagster_{table_slice.table}'
        drop_query = f'''
            DROP TABLE IF EXISTS {tmp_table_name}
        '''
        create_query = f'''
            CREATE TABLE {tmp_table_name}(
                {trino_columns}
            )
            WITH (
                format = 'PARQUET',
                location = '{table_dir}'
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


    def load_input(
        self, context: InputContext, table_slice: TableSlice, connection
    ) -> TableFilePaths:
        """Loads the paths of object storage files populating the table"""
        #TODO: work on partitioning
        query = f'''
            SELECT DISTINCT "$path" FROM {table_slice.schema}.{table_slice.table}
        '''
        try:
            res = connection.execute(query)
        except TrinoQueryError as e:
            # TODO add messaging around this functionality is supported only with the Hive connector
            raise e
        filepaths = [filepath for pathlist in res for filepath in pathlist]
        context.add_input_metadata({
            'file_paths': filepaths
        })
        return filepaths
    
    @property
    def supported_types(self):
        return [TableFilePaths, list]
    
    @property
    def requires_fsspec(self):
        return True
    
class ArrowTypeHandler(TrinoBaseTypeHandler):
    def __init__(self):
        self.file_handler = FilePathTypeHandler()


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
    
class PandasArrowTypeHandler(TrinoBaseTypeHandler):
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