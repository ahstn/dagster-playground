# Trino IO Manager

> [!INFO]
> For more info on IO managers in Dagster, see: [IO Managers | Dagster]: https://docs.dagster.io/concepts/io-management/io-managers


## Overview
Dagster's IO managers are responsible for storing and retrieving the inputs and outputs of the computations in your pipelines. They provide a layer of abstraction for reading and writing data, allowing you to switch between different storage systems (like local disk, cloud storage, or databases) without changing your business logic.

This module, inspired by [andreapiso/dagster-trino | GitHub], module extends Dagster's functionality by providing a custom IO manager. 

It is specifically designed use Pandas DataFrames to generate Parquet files, upload them to blob storage and reflect any tables in Trino and Hive Metastore. This approach is generally known as a "Data Lakehouse", allowing us to scale individual compute components, while taking advantage of inexpensive storage costs and Trino's powerful query engine.

## Usage

> [!WARNING]
> Urgent info that needs immediate user attention to avoid problems.

To use the trino_io_manager, you need to include it in your `resources` definition. Here's an example:

```python
from .trino_io_manager import build_trino_io_manager, TrinoIOManager

defs = Definitions(
    resources={
        "trino_io_manager": TrinoIOManager(
            # S3 Bucket to is - prefixes all generated parquet files
            bucket="warehouse",

            # Trino connection parameters to use
            # https://github.com/trinodb/trino-python-client
            connection_config={
                "catalog": "hive",
                "schema": "test",
                "user": "trino",
                "host": "localhost",
                "port": 8080,
            },

            # Intermediary storage format, only Parquet for now
            type_handler="parquet",

            # Generic FSSpec config - directly passed to `fsspec.filesystem()`
            # https://s3fs.readthedocs.io/en/latest/api.html
            fs_config={
                "protocol": "s3a",
                "key": "minio",
                "secret": "minio123",
                "endpoint_url": "http://localhost:9000"
            },
        )
    }
```

Behind the scenes `SQLAlchemy` is used in [`db_client.py] to create the connection to Trino, this client is then tied to our [`io_manager.py`]. While [`io_manager.py`] lays the foundation, the majority of heavy lifting is done in [`handlers/parquet.py`].

Different handlers can be created for accepting other formats, loading data via different means. `ParquetTypeHandler` firstly generates Parquet files using [`pyarrow`], which are passed to [`fsspec`] for uploading, depending on `"bucket"` in the example above.

Assuming the upload was successful, we translate Pandas datatypes match Trino's using [`handlers/data_type_mapping.py`] a build a traditional `CREATE TABLE (${columns}..)` statement. This is the last step, and once executed, the table will be available for querying in Trino.

[andreapiso/dagster-trino | GitHub]: https://github.com/andreapiso/dagster-trino
[`pyarrow`]: https://arrow.apache.org/docs/python/index.html
[`fsspec`]: https://filesystem-spec.readthedocs.io/en/latest/

[`db_client.py`]: ./db_client.py
[`io_manager.py`]: ./io_manager.py
[`handlers/parquet.py`]: handlers/parquet.py
[`handlers/parqdata_type_mappinguet.py`]: handlers/data_type_mapping.py