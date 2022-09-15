from tests.functional.queries.sql.boolean_functions_sql import (
    SQL_SCALAR, SQL_AGGREGATE
)

SCALAR_FUNCTIONS = (
    {
        "test_name": "or",
        "file_names": [],
        "sql_query": SQL_SCALAR["or"],
        "ibis_expr": None
    },
    {
        "test_name": "and",
        "file_names": [],
        "sql_query": SQL_SCALAR["and"],
        "ibis_expr": None
    },
    {
        "test_name": "not",
        "file_names": [],
        "sql_query": SQL_SCALAR["not"],
        "ibis_expr": None
    },
    {
        "test_name": "xor",
        "file_names": [],
        "sql_query": SQL_SCALAR["xor"],
        "ibis_expr": None
    },
)

AGGREGATE_FUNCTIONS = (
    {
        "test_name": "bool_and",
        "file_names": [],
        "sql_query": SQL_AGGREGATE["bool_and"],
        "ibis_expr": None
    },
    {
        "test_name": "bool_or",
        "file_names": [],
        "sql_query": SQL_AGGREGATE["bool_or"],
        "ibis_expr": None
    },
)
