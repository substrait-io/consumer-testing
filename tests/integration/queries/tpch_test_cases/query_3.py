import pytest

from ....common import get_sql, get_substrait_plan

TESTCASE = [
    {
        "test_name": "test_tpch_sql_3",
        "file_names": [
            "lineitem.parquet",
            "customer.parquet",
            "orders.parquet",
        ],
        "sql_query": get_sql("q3.sql"),
        "substrait_query": get_substrait_plan("query_3_plan.json"),
    }
]
