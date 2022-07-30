import pytest
from ....common import get_substrait_plan

TESTCASE = [
    {
        "test_name": "test_tpch_sql_5",
        "file_names": ["customer_0.1.parquet", "orders_0.1.parquet", "lineitem_0.1.parquet",
                       "supplier_0.1.parquet", "nation_0.1.parquet", "region_0.1.parquet"],
        "sql_query":
            """
            SELECT
                n_name,
                sum(l_extendedprice * (1 - l_discount)) AS revenue
            FROM
                '{}', '{}', '{}', '{}', '{}', '{}'
            WHERE
                c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND l_suppkey = s_suppkey
                AND c_nationkey = s_nationkey
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'ASIA'
                AND o_orderdate >= CAST('1994-01-01' AS date)
                AND o_orderdate < CAST('1995-01-01' AS date)
            GROUP BY
                n_name
            ORDER BY
                revenue DESC
            """,
        "substrait_query": get_substrait_plan('query_5_plan.json')
    }
]