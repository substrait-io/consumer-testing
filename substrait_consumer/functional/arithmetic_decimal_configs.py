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
        "ibis_expr": None,
    },
    {
        "test_name": "subtract",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["subtract"],
        "ibis_expr": None,
    },
    {
        "test_name": "multiply",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["multiply"],
        "ibis_expr": None,
    },
    {
        "test_name": "divide",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["divide"],
        "ibis_expr": None,
    },
    {
        "test_name": "modulus",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["modulus"],
        "ibis_expr": None,
    },
)

AGGREGATE_FUNCTIONS = (
    {
        "test_name": "sum",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["sum"],
        "ibis_expr": None,
    },
    {
        "test_name": "avg",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["avg"],
        "ibis_expr": None,
    },
    {
        "test_name": "min",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["min"],
        "ibis_expr": None,
    },
    {
        "test_name": "max",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["max"],
        "ibis_expr": None,
    },
)
