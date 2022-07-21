import pytest
from ....common import get_substrait_plan

TESTCASE = [
    {
        "test_name": "test_tpch_sql_4",
        "file_names": ["orders_1.parquet", "lineitem_1.parquet"],
        "sql_query":
            """
            SELECT
                o_orderpriority,
                count(*) AS order_count
            FROM
                '{}'
            WHERE
                o_orderdate >= CAST('1993-07-01' AS date)
                AND o_orderdate < CAST('1993-10-01' AS date)
                AND EXISTS (
                    SELECT
                        *
                    FROM
                        '{}'
                    WHERE
                        l_orderkey = o_orderkey
                        AND l_commitdate < l_receiptdate)
            GROUP BY
                o_orderpriority
            ORDER BY
                o_orderpriority;
            """,
        "substrait_query": get_substrait_plan('query_4_plan.json')
    }
]