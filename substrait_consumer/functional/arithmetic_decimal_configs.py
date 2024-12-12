from substrait_consumer.functional.queries.sql.arithmetic_demical_functions_sql import (
    SQL_SCALAR,
    SQL_AGGREGATE,
)

SCALAR_FUNCTIONS = (
    {
        "test_name": "add",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["add"],
    },
    {
        "test_name": "subtract",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["subtract"],
    },
    {
        "test_name": "multiply",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["multiply"],
    },
    {
        "test_name": "divide",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["divide"],
    },
    {
        "test_name": "modulus",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["modulus"],
    },
)

AGGREGATE_FUNCTIONS = (
    {
        "test_name": "sum",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["sum"],
    },
    {
        "test_name": "avg",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["avg"],
    },
    {
        "test_name": "min",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["min"],
    },
    {
        "test_name": "max",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["max"],
    },
)
