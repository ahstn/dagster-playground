# Trino & Hive Metastore

As I understand it, Hive Metastore used to be tightly coupled with the general Hive & Hadoop stack. 

Around 2012-2015, a gradual de-coupling occured making it less reliant on HFDS and Hadoop components. This came with a shift to using [Apache Thrift] as communication protocol and supporting more backing stores

While already successful as a "catalog" store for table metadata and data locations, this shift helped adoptions for emerging table formats.

## Integration

In `./docker/trino/catalog` we have a `hive.properties` file. The name of this is arbitrary, but it becomes the Trino Catalog name.

```properties
connector.name=hive
hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.path-style-access=true
hive.s3.endpoint=http://minio:9000
hive.s3.aws-access-key=minio
hive.s3.aws-secret-key=minio123
hive.non-managed-table-writes-enabled=true
hive.storage-format=ORC
hive.s3.ssl.enabled=false
hive.allow-drop-table=true
```

Now when Trino is started - `docker compose up trino`, we can run the following to ensure connectivity between Trino, Hive and Minio:

```sql
CREATE SCHEMA hive.raw with (LOCATION = 's3a://warehouse/');
CREATE TABLE hive.raw.user_data (
		id 			    int,
		first_name 		varchar,
		last_name 		varchar,
		email 			varchar
);

INSERT INTO hive.raw.user_data(id, first_name, last_name, email) VALUES (1, 'test', 'test', 'test@test.com');
```

If no errors are returned, you'll see the new schema and tables in your DB IDE of choice. Alternatively you can directly query Trino again:

```sql
USE hive.raw;
SHOW TABLES;
```

[Apache Thift]: https://en.wikipedia.org/wiki/Apache_Thrift