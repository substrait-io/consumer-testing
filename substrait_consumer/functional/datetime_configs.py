from substrait_consumer.functional.queries.sql.datetime_functions_sql import SQL_SCALAR

SCALAR_FUNCTIONS = (
    {
        "test_name": "extract",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["extract"],
        "ibis_expr": None,
    },
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
        "test_name": "lt",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["lt"],
        "ibis_expr": None,
    },
    {
        "test_name": "lte",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["lte"],
        "ibis_expr": None,
    },
    {
        "test_name": "gt",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["gt"],
        "ibis_expr": None,
    },
    {
        "test_name": "gte",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["gte"],
        "ibis_expr": None,
    },
    {
        "test_name": "add_intervals",
        "local_files": {},
        "named_tables": {"customer": "customer.parquet"},
        "sql_query": SQL_SCALAR["add_intervals"],
        "ibis_expr": None,
    },
)
