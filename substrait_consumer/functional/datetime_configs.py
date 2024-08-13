from substrait_consumer.functional.queries.sql.datetime_functions_sql import SQL_SCALAR

SCALAR_FUNCTIONS = (
    {
        "test_name": "extract",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["extract"],
        "ibis_expr": None,
    },
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
        "test_name": "lt",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["lt"],
        "ibis_expr": None,
    },
    {
        "test_name": "lte",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["lte"],
        "ibis_expr": None,
    },
    {
        "test_name": "gt",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["gt"],
        "ibis_expr": None,
    },
    {
        "test_name": "gte",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["gte"],
        "ibis_expr": None,
    },
    {
        "test_name": "add_intervals",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["add_intervals"],
        "ibis_expr": None,
    },
)
