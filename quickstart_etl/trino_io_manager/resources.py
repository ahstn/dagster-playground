from typing import Mapping, Iterator, Optional, Any
from contextlib import closing, contextmanager
import sys

from dagster import resource, Field, StringSource, ResourceDefinition
from dagster._annotations import public
import dagster._check as check

from .configs import define_trino_config

import pandas as pd

import trino
import fsspec
import os

class TrinoConnection:
    """A connection to Trino that can execute queries. In general this class should not be
    directly instantiated, but rather used as a resource in an op or asset.
    """
    def __init__(self, config: Mapping[str, str], log):  # pylint: disable=too-many-locals
        # Extract parameters from resource config.

        self.connector = config.get("connector", None)
        self.sqlalchemy_engine_args = {}
        self.pandas_trino_fix = None

        if self.connector == 'sqlalchemy':
            from sqlalchemy.engine import Connection
            from sqlalchemy import insert

            def _pandas_trino_fix(pd_table, conn: Connection, keys: list, data_iter: Iterator):
                """
                Custom function to hack around issue with Pandas adding trailing semi-colon.
                If the statement that Pandas generates contains a trailing semicolon, remove
                it before actually executing the query.
                """
                data = [dict(zip(keys, row)) for row in data_iter]
                executable = insert(pd_table.table).values(data) # sqlalchemy.sql.dml.Insert
                statement = str(executable.compile(dialect=conn.dialect, compile_kwargs={"literal_binds": True}))
                
                # remove the trailing semicolon if required.
                if statement.strip().endswith(';'):
                    statement = statement.rstrip(';', 1)

                # conn.execute can take a string or a `sqlalchemy.sql.expression.Executable`
                result = conn.execute(statement)
                return result.rowcount
            self.pandas_trino_fix = _pandas_trino_fix
            

            self.conn_args = {
                k: config.get(k)
                for k in (
                    "host",
                    "user",
                    "port",
                    "catalog",
                    "schema"
                )
                if config.get(k) is not None
            }
            self.sqlalchemy_engine_args['http_scheme'] = config.get('http_scheme', None)
        else:
            self.conn_args = {
                k: config.get(k)
                for k in (
                    "host",
                    "user",
                    "port",
                    "catalog",
                    "schema"
                )
                if config.get(k) is not None
            }

        self.log = log

    @public
    @contextmanager
    def get_connection(self) -> Iterator[trino.dbapi.Connection]:
        """Gets a connection to Trino as a context manager."""
        if self.connector == "sqlalchemy":
            from trino.sqlalchemy import URL
            from sqlalchemy import create_engine

            engine = create_engine(URL(**self.conn_args), connect_args=self.sqlalchemy_engine_args)
            conn = engine.connect()

            yield conn
            conn.close()
            engine.dispose()
        else:
            conn = trino.dbapi.connect(**self.conn_args)
            yield conn
            conn.close()

# ---

def _create_fsspec_filesystem(config) -> fsspec.spec.AbstractFileSystem:
    fsspec_params = dict(config)
    return fsspec.filesystem(**fsspec_params) 

class FsSpec:
    """A Class that creates an fsspec filesystem for the desired storage protocol. 
    In general this class should not be directly instantiated, but rather used as a 
    resource with a type handler that requires access to the Trino underlying storage.
    """
    def __init__(self, tmp_path, fsspec_params):
        if "protocol" not in fsspec_params:
            fsspec_params["protocol"] = "file"
        self.protocol = fsspec_params['protocol']
        self.tmp_folder = os.path.join(tmp_path, '_dagster_tmp')
        self.params = fsspec_params

    @public
    @contextmanager
    def get_fs(self):
        fs = _create_fsspec_filesystem(self.params)
        fs.makedirs(self.tmp_folder, exist_ok=True)
        yield fs
        del(fs)

        

def build_fsspec_resource(fsspec_params) -> ResourceDefinition:
    """
    Builder for fsspec filesystem with the given parameters.

    Arguments:

        fsspec_params(dict): Dictionary containing arguments to be 
            passed as-is to a `fsspec_filesystem` initiation. 
            if the `protocol` parameter is not set, it defaults to the 
            local storage protocol `file`.
    """
    @resource(config_schema={
        'tmp_path': Field(StringSource, is_required=True,
                        description="Path where to stage temporary files. \
                            Will create a _dagster_tmp folder if it does not exist") 
    })
    def fsspec_resource(context):
        # init_context.log.info(f"IOManager: {init_context.resource_config}")
        return FsSpec(context.resource_config['tmp_path'], fsspec_params)
    
    return fsspec_resource