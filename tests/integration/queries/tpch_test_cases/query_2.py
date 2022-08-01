import pytest
from ....common import get_substrait_plan, get_sql

TESTCASE = [
    {
        "test_name": "test_tpch_sql_2",
        "file_names": [
            "part_0.1.parquet",
            "supplier_0.1.parquet",
            "partsupp_0.1.parquet",
            "nation_0.1.parquet",
            "region_0.1.parquet",
            "partsupp_0.1.parquet",
            "supplier_0.1.parquet",
            "nation_0.1.parquet",
            "region_0.1.parquet",
        ],
        "sql_query": get_sql("q2.sql"),
        "substrait_query": get_substrait_plan("query_2_plan.json"),
    }
]
