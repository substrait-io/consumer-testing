from substrait_consumer.functional.queries.ibis_expressions.logarithmic_functions_expr import (
    IBIS_SCALAR)
from substrait_consumer.functional.queries.sql.logarithmic_functions_sql import SQL_SCALAR

SCALAR_FUNCTIONS = (
    {
        "test_name": "ln",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["ln"],
        "ibis_expr": IBIS_SCALAR["ln"],
    },
    {
        "test_name": "log10",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["log10"],
        "ibis_expr": IBIS_SCALAR["log10"],
    },
    {
        "test_name": "log2",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["log2"],
        "ibis_expr": IBIS_SCALAR["log2"],
    },
    {
        "test_name": "logb",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["logb"],
        "ibis_expr": IBIS_SCALAR["logb"],
    },
)
