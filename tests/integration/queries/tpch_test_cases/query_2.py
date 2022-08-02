import pytest

from ....common import get_sql, get_substrait_plan

TESTCASE = [
    {
        "test_name": "test_tpch_sql_2",
        "file_names": [
            "part.parquet",
            "supplier.parquet",
            "partsupp.parquet",
            "nation.parquet",
            "region.parquet",
            "partsupp.parquet",
            "supplier.parquet",
            "nation.parquet",
            "region.parquet",
        ],
        "sql_query": get_sql("q2.sql"),
        "substrait_query": get_substrait_plan("query_2_plan.json"),
    }
]
