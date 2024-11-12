from substrait_consumer.functional.queries.ibis_expressions.rounding_functions_expr import IBIS_SCALAR
from substrait_consumer.functional.queries.sql.rounding_functions_sql import SQL_SCALAR

SCALAR_FUNCTIONS = (
    {
        "test_name": "ceil",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["ceil"],
        "ibis_expr": IBIS_SCALAR["ceil"],
    },
    {
        "test_name": "floor",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["floor"],
        "ibis_expr": IBIS_SCALAR["floor"],
    },
    {
        "test_name": "round",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["round"],
        "ibis_expr": IBIS_SCALAR["round"],
    },
)
