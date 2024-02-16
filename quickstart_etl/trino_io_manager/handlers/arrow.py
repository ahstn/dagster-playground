trino_string = "VARCHAR"
trino_bool = "BOOLEAN"
trino_tinyint = "TINYINT"
trino_smallint = "SMALLINT"
trino_int = "INTEGER"
trino_bigint = "BIGINT"
trino_float = "REAL"
trino_double = "DOUBLE"
trino_decimal = "DECIMAL"
trino_date = "DATE"
trino_time = "TIMESTAMP"

arrow_string= "string"
arrow_bool = "bool"
arrow_tinyint = "int8"
arrow_smallint = "int16"
arrow_int = "int32"
arrow_bigint = "int64"
arrow_float = "float"
arrow_double = "double"
arrow_decimal = "decimal128"
arrow_date = "date32[day]"
arrow_time = "timestamp[ns]"

map_arrow_trino_types = {
    arrow_string:   trino_string,
    arrow_bool:     trino_bool,
    arrow_tinyint:  trino_tinyint,
    arrow_smallint: trino_smallint,
    arrow_int:      trino_int,
    arrow_bigint:   trino_bigint,
    arrow_float:    trino_float,
    arrow_double:   trino_double,
    arrow_decimal:  trino_decimal,
    arrow_date:     trino_date,
    arrow_time:     trino_time
}

    