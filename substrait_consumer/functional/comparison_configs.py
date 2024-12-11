from substrait_consumer.functional.queries.sql.comparison_functions_sql import SQL_SCALAR

SCALAR_FUNCTIONS = (
    {
        "test_name": "not_equal",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["not_equal"],
    },
    {
        "test_name": "equal",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["equal"],
    },
    {
        "test_name": "is_not_distinct_from",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["is_not_distinct_from"],
    },
    {
        "test_name": "lt",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["lt"],
    },
    {
        "test_name": "lte",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["lte"],
    },
    {
        "test_name": "gt",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["gt"],
    },
    {
        "test_name": "gte",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["gte"],
    },
    {
        "test_name": "is_not_null",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["is_not_null"],
    },
    {
        "test_name": "is_null",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["is_null"],
    },
    {
        "test_name": "is_nan",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["is_nan"],
    },
    {
        "test_name": "is_finite",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["is_finite"],
    },
    {
        "test_name": "is_infinite",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["is_infinite"],
    },
    {
        "test_name": "between",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["between"],
    },
    {
        "test_name": "coalesce",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["coalesce"],
    },
)
