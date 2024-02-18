from contextlib import contextmanager
from dagster import OutputContext
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import DbClient, TablePartitionDimension, TableSlice
from trino.exceptions import TrinoQueryError, TrinoUserError

from typing import Sequence, cast
from trino.sqlalchemy import URL
from sqlalchemy import create_engine, Connection, text
from sqlalchemy.exc import SQLAlchemyError


TRINO_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S" #TODO: check correctness

class TrinoDbClient(DbClient):
    """
    Trino Client class. Should not be used directly, rather through the
    `build_trino_io_manager` function.
    """
    @staticmethod
    @contextmanager
    def connect(context, table_slice):
        engine = create_engine(
            URL(
                **context.resource_config.get('connection_config')
            )
        )
        conn = engine.connect()
        yield conn
        conn.close()
        engine.dispose()
    
    @staticmethod
    def ensure_schema_exists(context: OutputContext, table_slice: TableSlice, connection: Connection) -> None:
        """
        Validate the schema exists, if not create it.
        For Iceberg, this includes specifying the remote fs location ahead of time.
        """
        bucket = context.resource_config.get('bucket')
        connection.execute(text(
            f"CREATE SCHEMA IF NOT EXISTS {table_slice.schema} WITH (LOCATION = 's3a://{bucket}/{table_slice.schema}')"
        ))

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"

        if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
            query = f"SELECT {col_str} FROM {table_slice.schema}.{table_slice.table} WHERE\n"
            return f'({query}{_partition_where_clause(table_slice.partition_dimensions)})'
        else:
            return f'(SELECT {col_str} FROM {table_slice.schema}.{table_slice.table})'
        
    @staticmethod
    def delete_table_slice(context: OutputContext, table_slice: TableSlice, connection) -> None:
        """
        Truncate table if it exists, if not ignore exceptions
        """
        try:
            connection.execute(text(_get_cleanup_statement(table_slice)))
        except TrinoUserError as e:
            if e.error_name == 'TABLE_NOT_FOUND': 
                pass
        except SQLAlchemyError as e:
            if "TABLE_NOT_FOUND" in e._message():
                pass


def _get_cleanup_statement(table_slice: TableSlice) -> str:
    """
    Returns a SQL statement that deletes data in the given table to make way for the output data
    being written.
    """
    if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
        query = f"DELETE FROM {table_slice.schema}.{table_slice.table} WHERE\n"
        return query + _partition_where_clause(table_slice.partition_dimensions)
    else:
        return f"DELETE FROM {table_slice.schema}.{table_slice.table}"
    
def _partition_where_clause(partition_dimensions: Sequence[TablePartitionDimension]) -> str:
    return " AND\n".join(
        _time_window_where_clause(partition_dimension)
        if isinstance(partition_dimension.partitions, TimeWindow)
        else _static_where_clause(partition_dimension)
        for partition_dimension in partition_dimensions
    )

def _time_window_where_clause(table_partition: TablePartitionDimension) -> str:
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    start_dt_str = start_dt.strftime(TRINO_DATETIME_FORMAT)
    end_dt_str = end_dt.strftime(TRINO_DATETIME_FORMAT)
    return f"""{table_partition.partition_expr} >= '{start_dt_str}' AND {table_partition.partition_expr} < '{end_dt_str}'"""

def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partitions = ", ".join(f"'{partition}'" for partition in table_partition.partitions)
    return f"""{table_partition.partition_expr} in ({partitions})"""