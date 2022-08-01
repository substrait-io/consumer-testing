import pytest
from ....common import get_substrait_plan, get_sql

TESTCASE = [
    {
        "test_name": "test_tpch_sql_5",
        "file_names": ["customer_0.1.parquet", "orders_0.1.parquet", "lineitem_0.1.parquet",
                       "supplier_0.1.parquet", "nation_0.1.parquet", "region_0.1.parquet"],
        "sql_query": get_sql('q5.sql'),
        "substrait_query": get_substrait_plan('query_5_plan.json')
    }
]