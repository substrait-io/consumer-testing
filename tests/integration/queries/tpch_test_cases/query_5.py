import pytest

from ....common import get_sql, get_substrait_plan

TESTCASE = [
    {
        "test_name": "test_tpch_sql_5",
        "file_names": [
            "customer.parquet",
            "orders.parquet",
            "lineitem.parquet",
            "supplier.parquet",
            "nation.parquet",
            "region.parquet",
        ],
        "sql_query": get_sql("q5.sql"),
        "substrait_query": get_substrait_plan("query_5_plan.json"),
    }
]
