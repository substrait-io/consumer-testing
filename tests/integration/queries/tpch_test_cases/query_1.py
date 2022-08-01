import pytest
from ....common import get_substrait_plan, get_sql

TESTCASE = [
    {
        "test_name": "test_tpch_sql_1",
        "file_names": ["lineitem_0.1.parquet"],
        "sql_query": get_sql("q1.sql"),
        "substrait_query": get_substrait_plan("query_1_plan.json"),
    }
]
