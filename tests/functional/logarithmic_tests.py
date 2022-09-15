from tests.functional.queries.sql.logarithmic_functions_sql import (
    SQL_SCALAR,
)

SCALAR_FUNCTIONS = (
    {
        "test_name": "ln",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["ln"],
        "ibis_expr": None
    },
    {
        "test_name": "log10",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["log10"],
        "ibis_expr": None
    },
    {
        "test_name": "log2",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["log2"],
        "ibis_expr": None
    },
    {
        "test_name": "logb",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["logb"],
        "ibis_expr": None
    },
)
