import os
import pandas as pd
import pyarrow as pa
from dagster import InputContext, OutputContext
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from pyarrow import parquet, Schema, Table
from sqlalchemy import text

class DeltaLakeHandler(DbTypeHandler):
    def handle_output(self, context: OutputContext, table_slice: TableSlice, obj: pd.DataFrame, connection):
        file_path = os.path.join("delta-notebook", "iris")
        fs.makedirs(os.path.dirname(file_path), exist_ok=True)

        write_deltalake(file_path, dataframe, mode='overwrite')
        fs.put(file_path, "s3a://warehouse/delta-notebook/iris", recursive=True)

        with connection as conn:
            conn.execute(text(f"""
                CALL delta.system.register_table(
                    schema_name => 'notebook', 
                    table_name => 'iris', 
                    table_location => 's3a://warehouse/delta-notebook/iris'
                )
            """))
            conn.commit()
            
    def load_input(self, context: InputContext, table_slice: TableSlice, connection):
        return self.arrow_handler.load_input(context, table_slice, connection).to_pandas()
    
    @property
    def supported_types(self):
        return [pd.DataFrame]
    
    @property
    def requires_fsspec(self):
        return True