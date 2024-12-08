from substrait_consumer.functional.queries.ibis_expressions.arithmetic_functions_expr import (
    IBIS_AGGREGATE, IBIS_SCALAR)
from substrait_consumer.functional.queries.sql.arithmetic_functions_sql import (
    SQL_AGGREGATE, SQL_SCALAR)

SCALAR_FUNCTIONS = (
    {
        "test_name": "add",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["add"],
        "ibis_expr": IBIS_SCALAR["add"],
    },
    {
        "test_name": "subtract",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["subtract"],
        "ibis_expr": IBIS_SCALAR["subtract"],
    },
    {
        "test_name": "multiply",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["multiply"],
        "ibis_expr": IBIS_SCALAR["multiply"],
    },
    {
        "test_name": "divide",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["divide"],
        "ibis_expr": IBIS_SCALAR["divide"],
    },
    {
        "test_name": "modulus",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["modulus"],
        "ibis_expr": IBIS_SCALAR["modulus"],
    },
    {
        "test_name": "factorial",
        "local_files": {},
        "named_tables": {"nation": "nation.parquet"},
        "sql_query": SQL_SCALAR["factorial"],
        "ibis_expr": None,
    },
    {
        "test_name": "power",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["power"],
        "ibis_expr": IBIS_SCALAR["power"],
    },
    {
        "test_name": "sqrt",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["sqrt"],
        "ibis_expr": IBIS_SCALAR["sqrt"],
    },
    {
        "test_name": "exp",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["exp"],
        "ibis_expr": IBIS_SCALAR["exp"],
    },
    {
        "test_name": "negate",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["negate"],
        "ibis_expr": IBIS_SCALAR["negate"],
    },
    {
        "test_name": "cos",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["cos"],
        "ibis_expr": IBIS_SCALAR["cos"],
    },
    {
        "test_name": "acos",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["acos"],
        "ibis_expr": IBIS_SCALAR["acos"],
    },
    {
        "test_name": "sin",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["sin"],
        "ibis_expr": IBIS_SCALAR["sin"],
    },
    {
        "test_name": "asin",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["asin"],
        "ibis_expr": IBIS_SCALAR["asin"],
    },
    {
        "test_name": "tan",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["tan"],
        "ibis_expr": IBIS_SCALAR["tan"],
    },
    {
        "test_name": "atan",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["atan"],
        "ibis_expr": IBIS_SCALAR["atan"],
    },
    {
        "test_name": "atan2",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": SQL_SCALAR["atan2"],
        "ibis_expr": None,
    },
    {
        "test_name": "abs",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["abs"],
        "ibis_expr": IBIS_SCALAR["abs"],
    },
    {
        "test_name": "sign",
        "local_files": {},
        "named_tables": {},
        "sql_query": SQL_SCALAR["sign"],
        "ibis_expr": IBIS_SCALAR["sign"],
    },
)


AGGREGATE_FUNCTIONS = (
    {
        "test_name": "sum",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["sum"],
        "ibis_expr": IBIS_AGGREGATE["sum"],
    },
    {
        "test_name": "count",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["count"],
        "ibis_expr": None,
    },
    {
        "test_name": "count_star",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["count_star"],
        "ibis_expr": None,
    },
    {
        "test_name": "avg",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["avg"],
        "ibis_expr": IBIS_AGGREGATE["avg"],
    },
    {
        "test_name": "min",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["min"],
        "ibis_expr": IBIS_AGGREGATE["min"],
    },
    {
        "test_name": "max",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["max"],
        "ibis_expr": IBIS_AGGREGATE["max"],
    },
    {
        "test_name": "median",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["median"],
        "ibis_expr": IBIS_AGGREGATE["median"],
    },
    {
        "test_name": "mode",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["mode"],
        "ibis_expr": None,
    },
    {
        "test_name": "product",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["product"],
        "ibis_expr": None,
    },
    {
        "test_name": "std_dev",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["std_dev"],
        "ibis_expr": None,
    },
    {
        "test_name": "variance",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_AGGREGATE["variance"],
        "ibis_expr": None,
    },
)
