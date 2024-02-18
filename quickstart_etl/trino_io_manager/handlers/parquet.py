import os
import pandas as pd
import pyarrow as pa
from dagster import InputContext, OutputContext
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pyarrow import parquet, Schema
from sqlalchemy import text
from typing import Union
from fsspec import filesystem

    
class ParquetTypeHandler(DbTypeHandler):
    def upload_files(self, context: OutputContext, table_slice: TableSlice, obj: Union[pd.DataFrame, pa.Table]) -> str:
        """
        Using fsspec, upload the parquet files to the storage layer
        """
        file_path = os.path.join(
            f"s3a://{context.resource_config.get('bucket')}", 
            table_slice.schema, 
            f"{table_slice.table}.parquet"
        )
        if isinstance(obj, pd.DataFrame):
            obj = pa.Table.from_pandas(obj)
            
        fs = filesystem(**context.resource_config.get("fs_config"))
        fs.makedirs(os.path.dirname(file_path), exist_ok=True)
        parquet.write_table(obj, file_path, filesystem=fs)

        return os.path.dirname(file_path)
        
    def build_columns_string(self, obj: Union[pd.DataFrame, pa.Table]) -> str:
        """
        Map column types [pandas || pyarrow] -> [trino] using arrows
        """
        
        schema = Schema.from_pandas(obj) if isinstance(obj, pd.DataFrame) else obj.schema    
        # TODO: Add more types - ./arrow.py
        map_arrow_trino_types = {"string": "VARCHAR", "double": "DOUBLE"}

        return ", ".join([
            f"{column} {map_arrow_trino_types[str(dtype)]}" 
            for column, dtype in zip(schema.names, schema.types)
        ])

    def handle_output(self, context: OutputContext, ts: TableSlice, obj: pd.DataFrame, connection):
        context.log.info(f"No data to load into Trino, {obj} is empty")
        if len(obj) == 0:
            raise ValueError("No data to load into Trino")
        table_dir = self.upload_files(context, ts, obj)
        context.log.info(f"No data to load into Trino, {obj} is empty")
        columns = self.build_columns_string(obj)
    
        try:
            with connection as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {ts.schema}.{ts.table}"))
                conn.execute(text(f"""
                    CREATE TABLE {ts.schema}.{ts.table} ( {columns} )
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
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"

        return pd.read_sql(
            f"""SELECT {col_str} FROM {table_slice.schema}.{table_slice.table}""",
            connection
        )
    
    @property
    def supported_types(self):
        return [pa.Table, pd.DataFrame]
    
    @property
    def requires_fsspec(self):
        return True