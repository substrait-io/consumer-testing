from substrait_consumer.functional.queries.sql.string_functions_sql import SQL_SCALAR, SQL_AGGREGATE

SCALAR_FUNCTIONS = (
    {
        "test_name": "concat",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["concat"],
    },
    {
        "test_name": "concat_ws",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["concat_ws"],
    },
    {
        "test_name": "like",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["like"],
    },
    {
        "test_name": "starts_with0",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["starts_with"],
    },
    {
        "test_name": "starts_with1",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["starts_with_duckdb"],
    },
    {
        "test_name": "ends_with",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["ends_with"],
    },
    {
        "test_name": "substring0",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["substring"],
    },
    {
        "test_name": "substring1",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["substring_isthmus"],
    },
    {
        "test_name": "contains",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["contains"],
    },
    {
        "test_name": "strpos",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["strpos"],
    },
    {
        "test_name": "replace",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["replace"],
    },
    {
        "test_name": "repeat",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["repeat"],
    },
    {
        "test_name": "reverse",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["reverse"],
    },
    {
        "test_name": "lower",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["lower"],
    },
    {
        "test_name": "upper",
        "local_files": {},
        "named_tables": {"orders": "orders.parquet"},
        "sql_query": SQL_SCALAR["upper"],
    },
    {
        "test_name": "char_length",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["char_length"],
    },
    {
        "test_name": "bit_length",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["bit_length"],
    },
    {
        "test_name": "ltrim",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["ltrim"],
    },
    {
        "test_name": "rtrim",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["rtrim"],
    },
    {
        "test_name": "trim",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["trim"],
    },
    {
        "test_name": "lpad",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["lpad"],
    },
    {
        "test_name": "rpad",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["rpad"],
    },
    {
        "test_name": "left",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["left"],
    },
    {
        "test_name": "right",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["right"],
    },
)

AGGREGATE_FUNCTIONS = (
    {
        "test_name": "string_agg",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_AGGREGATE["string_agg"],
    },
)
