from tests.functional.queries.sql.comparison_functions_sql import (
    SQL_SCALAR
)

SCALAR_FUNCTIONS = (
    {
        "test_name": "not_equal",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["not_equal"],
        "ibis_expr": None
    },
    {
        "test_name": "equal",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["equal"],
        "ibis_expr": None
    },
    {
        "test_name": "is_not_distinct_from",
        "file_names": [],
        "sql_query": SQL_SCALAR["is_not_distinct_from"],
        "ibis_expr": None
    },
    {
        "test_name": "lt",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["lt"],
        "ibis_expr": None
    },
    {
        "test_name": "lte",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["lte"],
        "ibis_expr": None
    },
    {
        "test_name": "gt",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["gt"],
        "ibis_expr": None
    },
    {
        "test_name": "gte",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["gte"],
        "ibis_expr": None
    },
    {
        "test_name": "is_not_null",
        "file_names": [],
        "sql_query": SQL_SCALAR["is_not_null"],
        "ibis_expr": None
    },
    {
        "test_name": "is_null",
        "file_names": [],
        "sql_query": SQL_SCALAR["is_null"],
        "ibis_expr": None
    },
    {
        "test_name": "is_nan",
        "file_names": [],
        "sql_query": SQL_SCALAR["is_nan"],
        "ibis_expr": None
    },
    {
        "test_name": "is_finite",
        "file_names": [],
        "sql_query": SQL_SCALAR["is_finite"],
        "ibis_expr": None
    },
    {
        "test_name": "is_infinite",
        "file_names": [],
        "sql_query": SQL_SCALAR["is_infinite"],
        "ibis_expr": None
    },
    {
        "test_name": "coalesce",
        "file_names": [],
        "sql_query": SQL_SCALAR["coalesce"],
        "ibis_expr": None
    }
)
