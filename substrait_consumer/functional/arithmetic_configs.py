from substrait_consumer.functional.queries.ibis_expressions.arithmetic_functions_expr import (
    IBIS_AGGREGATE, IBIS_SCALAR)
from substrait_consumer.functional.queries.sql.arithmetic_functions_sql import (
    SQL_AGGREGATE, SQL_SCALAR)

SCALAR_FUNCTIONS = (
    {
        "test_name": "add",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["add"],
        "ibis_expr": IBIS_SCALAR["add"],
    },
    {
        "test_name": "subtract",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["subtract"],
        "ibis_expr": IBIS_SCALAR["subtract"],
    },
    {
        "test_name": "multiply",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["multiply"],
        "ibis_expr": IBIS_SCALAR["multiply"],
    },
    {
        "test_name": "divide",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["divide"],
        "ibis_expr": IBIS_SCALAR["divide"],
    },
    {
        "test_name": "modulus",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["modulus"],
        "ibis_expr": IBIS_SCALAR["modulus"],
    },
    {
        "test_name": "factorial",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["factorial"],
        "ibis_expr": None,
    },
    {
        "test_name": "power",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["power"],
        "ibis_expr": IBIS_SCALAR["power"],
    },
    {
        "test_name": "sqrt",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["sqrt"],
        "ibis_expr": IBIS_SCALAR["sqrt"],
    },
    {
        "test_name": "exp",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["exp"],
        "ibis_expr": IBIS_SCALAR["exp"],
    },
    {
        "test_name": "negate",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["negate"],
        "ibis_expr": IBIS_SCALAR["negate"],
    },
    {
        "test_name": "cos",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["cos"],
        "ibis_expr": IBIS_SCALAR["cos"],
    },
    {
        "test_name": "acos",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["acos"],
        "ibis_expr": IBIS_SCALAR["acos"],
    },
    {
        "test_name": "sin",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["sin"],
        "ibis_expr": IBIS_SCALAR["sin"],
    },
    {
        "test_name": "asin",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["asin"],
        "ibis_expr": IBIS_SCALAR["asin"],
    },
    {
        "test_name": "tan",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["tan"],
        "ibis_expr": IBIS_SCALAR["tan"],
    },
    {
        "test_name": "atan",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["atan"],
        "ibis_expr": IBIS_SCALAR["atan"],
    },
    {
        "test_name": "atan2",
        "file_names": ["lineitem.parquet"],
        "sql_query": SQL_SCALAR["atan2"],
        "ibis_expr": None,
    },
    {
        "test_name": "abs",
        "file_names": [],
        "sql_query": SQL_SCALAR["abs"],
        "ibis_expr": IBIS_SCALAR["abs"],
    },
    {
        "test_name": "sign",
        "file_names": [],
        "sql_query": SQL_SCALAR["sign"],
        "ibis_expr": IBIS_SCALAR["sign"],
    },
)


AGGREGATE_FUNCTIONS = (
    {
        "test_name": "sum",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["sum"],
        "ibis_expr": IBIS_AGGREGATE["sum"],
    },
    {
        "test_name": "count",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["count"],
        "ibis_expr": None,
    },
    {
        "test_name": "count_star",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["count_star"],
        "ibis_expr": None,
    },
    {
        "test_name": "avg",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["avg"],
        "ibis_expr": IBIS_AGGREGATE["avg"],
    },
    {
        "test_name": "min",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["min"],
        "ibis_expr": IBIS_AGGREGATE["min"],
    },
    {
        "test_name": "max",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["max"],
        "ibis_expr": IBIS_AGGREGATE["max"],
    },
    {
        "test_name": "median",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["median"],
        "ibis_expr": IBIS_AGGREGATE["median"],
    },
    {
        "test_name": "mode",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["mode"],
        "ibis_expr": None,
    },
    {
        "test_name": "product",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["product"],
        "ibis_expr": None,
    },
    {
        "test_name": "std_dev",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["std_dev"],
        "ibis_expr": None,
    },
    {
        "test_name": "variance",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_AGGREGATE["variance"],
        "ibis_expr": None,
    },
)
