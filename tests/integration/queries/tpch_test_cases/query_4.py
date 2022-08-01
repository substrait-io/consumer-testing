import pytest
from ....common import get_substrait_plan, get_sql

TESTCASE = [
    {
        "test_name": "test_tpch_sql_4",
        "file_names": ["orders_0.1.parquet", "lineitem_0.1.parquet"],
        "sql_query": get_sql('q4.sql'),
        "substrait_query": get_substrait_plan('query_4_plan.json')
    }
]