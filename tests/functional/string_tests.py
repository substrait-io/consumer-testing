from tests.functional.queries.sql.string_functions_sql import SQL_SCALAR, SQL_AGGREGATE

SCALAR_FUNCTIONS = (
    {
        "test_name": "concat",
        "file_names": ["nation.parquet", "region.parquet"],
        "sql_query": SQL_SCALAR["concat"],
        "ibis_expr": None,
    },
    {
        "test_name": "concat_ws",
        "file_names": ["nation.parquet", "region.parquet"],
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
        "ibis_expr": None,
    },
    {
        "test_name": "ends_with",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["ends_with"],
        "ibis_expr": None,
    },
    {
        "test_name": "substring",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["substring"],
        "ibis_expr": None,
    },
    {
        "test_name": "contains",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["contains"],
        "ibis_expr": None,
    },
    {
        "test_name": "strpos",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["strpos"],
        "ibis_expr": None,
    },
    {
        "test_name": "replace",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["replace"],
        "ibis_expr": None,
    },
    {
        "test_name": "repeat",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["repeat"],
        "ibis_expr": None,
    },
    {
        "test_name": "reverse",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["reverse"],
        "ibis_expr": None,
    },
    {
        "test_name": "lower",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["lower"],
        "ibis_expr": None,
    },
    {
        "test_name": "upper",
        "file_names": ["orders.parquet"],
        "sql_query": SQL_SCALAR["upper"],
        "ibis_expr": None,
    },
    {
        "test_name": "char_length",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["char_length"],
        "ibis_expr": None,
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
        "ibis_expr": None,
    },
    {
        "test_name": "rtrim",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["rtrim"],
        "ibis_expr": None,
    },
    {
        "test_name": "trim",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["trim"],
        "ibis_expr": None,
    },
    {
        "test_name": "lpad",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["lpad"],
        "ibis_expr": None,
    },
    {
        "test_name": "rpad",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["rpad"],
        "ibis_expr": None,
    },
    {
        "test_name": "left",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["left"],
        "ibis_expr": None,
    },
    {
        "test_name": "right",
        "file_names": ["nation.parquet"],
        "sql_query": SQL_SCALAR["right"],
        "ibis_expr": None,
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
