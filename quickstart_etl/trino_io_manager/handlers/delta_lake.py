import pandas as pd
from os.path import dirname, join as path_join
from dagster import InputContext, OutputContext
from dagster._core.storage.db_io_manager import DbTypeHandler, TableSlice
from sqlalchemy import text
from fsspec import filesystem
from deltalake import write_deltalake

class DeltaLakeHandler(DbTypeHandler):
    def handle_output(self, context: OutputContext, table_slice: TableSlice, df: pd.DataFrame, connection):
        """
        Using Delta Lake, write the dataframe to a local directly. (Deltas is multiple files)
        Then, upload the local directory remotely using FSSpec and update trino.
        """
        local_path = path_join("target", table_slice.schema, table_slice.table)
        fs = filesystem(**context.resource_config.get("fs_config"))
        fs.makedirs(dirname(local_path), exist_ok=True)
        write_deltalake(local_path, df, mode='overwrite')

        remote_path = path_join("s3a://warehouse", "delta", table_slice.schema, table_slice.table)
        fs.put(local_path, remote_path, recursive=True)

        context.add_output_metadata({
            "local_path": local_path,
            remote_path: remote_path
        })

        with connection as conn:
            conn.execute(text(f"""
                CALL delta.system.register_table(
                    schema_name => '{table_slice.schema}', 
                    table_name => '{table_slice.table}', 
                    table_location => '{remote_path}'
                )
            """))
            conn.commit()
            
    def load_input(self, context: InputContext, table_slice: TableSlice, connection):
        return pd.read_sql(f"SELECT * FROM {table_slice.schema}.{table_slice.table}", connection)
    
    @property
    def supported_types(self):
        return [pd.DataFrame]
    
    @property
    def requires_fsspec(self):
        return True