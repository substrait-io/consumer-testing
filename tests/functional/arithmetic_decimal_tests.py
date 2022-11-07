from tests.functional.queries.sql.arithmetic_demical_functions_sql import (
    SQL_SCALAR,
    SQL_AGGREGATE,
)

SCALAR_FUNCTIONS = (
    {
        "test_name": "add",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["add"],
        "ibis_expr": None,
    },
    {
        "test_name": "subtract",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["subtract"],
        "ibis_expr": None,
    },
    {
        "test_name": "multiply",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["multiply"],
        "ibis_expr": None,
    },
    {
        "test_name": "divide",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["divide"],
        "ibis_expr": None,
    },
    {
        "test_name": "modulus",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["modulus"],
        "ibis_expr": None,
    },
)

AGGREGATE_FUNCTIONS = (
    {
        "test_name": "sum",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_AGGREGATE["sum"],
        "ibis_expr": None,
    },
    {
        "test_name": "avg",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_AGGREGATE["avg"],
        "ibis_expr": None,
    },
    {
        "test_name": "min",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_AGGREGATE["min"],
        "ibis_expr": None,
    },
    {
        "test_name": "max",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_AGGREGATE["max"],
        "ibis_expr": None,
    },
)
