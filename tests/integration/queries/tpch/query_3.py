import pytest
from ....common import get_substrait_plan

TESTCASE = [
    {
        "test_name": "test_tpch_sql_3",
        "file_names": ["lineitem_1.parquet", "customer_1.parquet", "orders_1.parquet"],
        "sql_query":
            """
            SELECT
                l_orderkey,
                sum(l_extendedprice * (1 - l_discount)) AS revenue,
                o_orderdate,
                o_shippriority
            FROM
                '{}', '{}', '{}'
            WHERE
                c_mktsegment = 'BUILDING'
                AND c_custkey = o_custkey
                AND l_orderkey = o_orderkey
                AND o_orderdate < CAST('1995-03-15' AS date)
                AND l_shipdate > CAST('1995-03-15' AS date)
            GROUP BY
                l_orderkey,
                o_orderdate,
                o_shippriority
            ORDER BY
                revenue DESC,
                o_orderdate
            LIMIT 10;            
        """,
        "substrait_query": get_substrait_plan('query_3_plan.json')
    }
]