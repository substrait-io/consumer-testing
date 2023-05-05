from substrait_consumer.functional.queries.sql.string_functions_sql import SQL_SCALAR, SQL_AGGREGATE
from substrait_consumer.functional.queries.ibis_expressions.string_functions_expr import (
    IBIS_SCALAR)

SCALAR_FUNCTIONS = (
    {
        "test_name": "concat",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["concat"],
        "ibis_expr": IBIS_SCALAR["concat"],
    },
    {
        "test_name": "concat_ws",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["concat_ws"],
        "ibis_expr": None,
    },
    {
        "test_name": "like",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["like"],
        "ibis_expr": None,
    },
    {
        "test_name": "starts_with",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["starts_with"],
        "ibis_expr": IBIS_SCALAR["starts_with"],
    },
    {
        "test_name": "ends_with",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["ends_with"],
        "ibis_expr": IBIS_SCALAR["ends_with"],
    },
    {
        "test_name": "substring",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["substring"],
        "ibis_expr": IBIS_SCALAR["substr"],
    },
    {
        "test_name": "substring",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["substring_isthmus"],
        "ibis_expr": None,
    },
    {
        "test_name": "contains",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["contains"],
        "ibis_expr": IBIS_SCALAR["contains"],
    },
    {
        "test_name": "strpos",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["strpos"],
        "ibis_expr": IBIS_SCALAR["strpos"],
    },
    {
        "test_name": "replace",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["replace"],
        "ibis_expr": IBIS_SCALAR["replace"],
    },
    {
        "test_name": "repeat",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["repeat"],
        "ibis_expr": IBIS_SCALAR["repeat"],
    },
    {
        "test_name": "reverse",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["reverse"],
        "ibis_expr": IBIS_SCALAR["reverse"],
    },
    {
        "test_name": "lower",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["lower"],
        "ibis_expr": IBIS_SCALAR["lower"],
    },
    {
        "test_name": "upper",
        "file_names": ["orders.parquet"],
        "sql_query": SQL_SCALAR["upper"],
        "ibis_expr": IBIS_SCALAR["upper"],
    },
    {
        "test_name": "char_length",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["char_length"],
        "ibis_expr": IBIS_SCALAR["char_length"],
    },
    {
        "test_name": "bit_length",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["bit_length"],
        "ibis_expr": None,
    },
    {
        "test_name": "ltrim",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["ltrim"],
        "ibis_expr": IBIS_SCALAR["ltrim"],
    },
    {
        "test_name": "rtrim",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["rtrim"],
        "ibis_expr": IBIS_SCALAR["rtrim"],
    },
    {
        "test_name": "trim",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["trim"],
        "ibis_expr": IBIS_SCALAR["trim"],
    },
    {
        "test_name": "lpad",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["lpad"],
        "ibis_expr": IBIS_SCALAR["lpad"],
    },
    {
        "test_name": "rpad",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["rpad"],
        "ibis_expr": IBIS_SCALAR["rpad"],
    },
    {
        "test_name": "left",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["left"],
        "ibis_expr": IBIS_SCALAR["left"],
    },
    {
        "test_name": "right",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["right"],
        "ibis_expr": IBIS_SCALAR["right"],
    },
)

AGGREGATE_FUNCTIONS = (
    {
        "test_name": "string_agg",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_AGGREGATE["string_agg"],
        "ibis_expr": None,
    },
)
