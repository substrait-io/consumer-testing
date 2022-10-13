from tests.functional.queries.sql.rounding_functions_sql import SQL_SCALAR

SCALAR_FUNCTIONS = (
    {
        "test_name": "ceil",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["ceil"],
        "ibis_expr": None,
    },
    {
        "test_name": "floor",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["floor"],
        "ibis_expr": None,
    },
)
