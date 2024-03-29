
# Arrow(left) to Trino(right) data type mapping
map_arrow_trino_types = {
    "string":           "VARCHAR",
    "bool":             "BOOLEAN",
    "int8":             "TINYINT",
    "int16":            "SMALLINT",
    "int32":            "INTEGER",
    "int64":            "BIGINT",
    "float":            "REAL",
    "double":           "DOUBLE",
    "decimal128":       "DECIMAL",
    "date32[day]":      "DATE",
    "timestamp[ns]":    "TIMESTAMP",
    "timestamp[ns, tz=UTC]": "TIMESTAMP",
}

# Pandas(left) to Trino(right) data type mapping
map_pandas_trino_types = {
    "string": "VARCHAR",
    "bool": "BOOLEAN",
    "int8": "TINYINT",
    "int16": "SMALLINT",
    "int32": "INTEGER",
    "int64": "BIGINT",
    "float": "REAL",
    "float64": "REAL",
    "object": "VARCHAR",
    "double": "DOUBLE",
    "decimal128": "DECIMAL",
    "date32[day]": "DATE",
    "datetime64[ns]": "TIMESTAMP",
    "datetime64[ns, UTC]": "TIMESTAMP",
}