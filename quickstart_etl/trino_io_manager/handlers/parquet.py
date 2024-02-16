import os
import pandas as pd
import pyarrow as pa
from dagster import InputContext, OutputContext
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pyarrow import parquet, Schema
from sqlalchemy import text
from typing import Union

    
class ParquetTypeHandler(DbTypeHandler):
    def upload_files(self, context: OutputContext, table_slice: TableSlice, obj: Union[pd.DataFrame, pa.Table]) -> str:
        """
        Using fsspec, upload the parquet files to the storage layer
        """
        file_path = os.path.join(
            f"s3a://{context.resources.fsspec.tmp_folder}", 
            table_slice.schema,
            f"{table_slice.table}.parquet"
        )
        if isinstance(obj, pd.DataFrame):
            obj = pa.Table.from_pandas(obj)
            
        with context.resources.fsspec.get_fs() as fs:
            fs.makedirs(os.path.dirname(file_path), exist_ok=True)

            parquet.write_table(obj, file_path, filesystem=fs)
            return os.path.dirname(file_path)
        
    def build_columns_string(self, obj: Union[pd.DataFrame, pa.Table]) -> str:
        """
        Map column types [pandas || pyarrow] -> [trino] using arrows
        """
        schema = Schema.from_pandas(obj[0]) if isinstance(obj, pd.DataFrame) else obj.schema    
        map_arrow_trino_types = {"string": "VARCHAR", "double": "DOUBLE"}

        return ", ".join([
            f"{column} {map_arrow_trino_types[str(dtype)]}" 
            for column, dtype in zip(schema.names, schema.types)
        ])

    def handle_output(self, context: OutputContext, table_slice: TableSlice, obj: pd.DataFrame, connection):
        table_dir = self.upload_files(context, table_slice, obj)
        columns = self.build_columns_string(obj)
    
        try:
            with connection as conn:
                conn.execute(text("DROP TABLE IF EXISTS public.iris"))
                conn.execute(text(f"""
                    CREATE TABLE public.iris ( {columns} )
                    WITH (
                        format = 'PARQUET', 
                        external_location = '{table_dir}'
                    )
                """))
                conn.commit()
        except Exception as e:
            context.log.error(f"Error while loading the parquet files into Trino: {e}")
            raise e

    def load_input(self, context: InputContext, table_slice: TableSlice, connection) -> pa.Table:
        if table_slice.partition_dimensions and len(context.asset_partition_keys) == 0:
            return pa.Table()
        file_paths = self.file_handler.load_input(context, table_slice, connection)

        with context.resources.fsspec.get_fs() as fs:
            arrow_df = parquet.ParquetDataset(file_paths, filesystem=fs)
        return arrow_df.read()
    
    @property
    def supported_types(self):
        return [pa.Table, pd.DataFrame]
    
    @property
    def requires_fsspec(self):
        return True