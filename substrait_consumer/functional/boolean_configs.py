from substrait_consumer.functional.queries.sql.boolean_functions_sql import SQL_SCALAR, SQL_AGGREGATE

SCALAR_FUNCTIONS = (
    {
        "test_name": "or",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["or"],
    },
    {
        "test_name": "and",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["and"],
    },
    {
        "test_name": "not",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["not"],
    },
    {
        "test_name": "xor",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["xor"],
    },
)

AGGREGATE_FUNCTIONS = (
    {
        "test_name": "bool_and",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_AGGREGATE["bool_and"],
    },
    {
        "test_name": "bool_or",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_AGGREGATE["bool_or"],
    },
)
