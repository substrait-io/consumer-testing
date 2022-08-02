import pytest

from ....common import get_sql, get_substrait_plan

TESTCASE = [
    {
        "test_name": "test_tpch_sql_1",
        "file_names": ["lineitem.parquet"],
        "sql_query": get_sql("q1.sql"),
        "substrait_query": get_substrait_plan("query_1_plan.json"),
    }
]
