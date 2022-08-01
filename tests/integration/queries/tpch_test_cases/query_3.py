import pytest
from ....common import get_substrait_plan, get_sql

TESTCASE = [
    {
        "test_name": "test_tpch_sql_3",
        "file_names": [
            "lineitem_0.1.parquet",
            "customer_0.1.parquet",
            "orders_0.1.parquet",
        ],
        "sql_query": get_sql("q3.sql"),
        "substrait_query": get_substrait_plan("query_3_plan.json"),
    }
]
