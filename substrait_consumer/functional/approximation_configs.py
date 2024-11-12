from substrait_consumer.functional.queries.sql.approximation_functions_sql import SQL_AGGREGATE

AGGREGATE_FUNCTIONS = (
    {
        "test_name": "approx_count_distinct",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["approx_count_distinct"],
        "ibis_expr": None,
    },
    {
        "test_name": "approx_distinct",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_AGGREGATE["approx_distinct"],
        "ibis_expr": None,
    },
)
