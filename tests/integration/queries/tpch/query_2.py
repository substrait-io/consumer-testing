import pytest
from ....common import get_substrait_plan

TESTCASE = [
    {
        "test_name": "test_tpch_sql_2",
        "file_names": ["part_1.parquet", "supplier_1.parquet",
                       "partsupp_1.parquet", "nation_1.parquet",
                       "region_1.parquet", "partsupp_1.parquet",
                       "supplier_1.parquet", "nation_1.parquet",
                       "region_1.parquet"],
        "sql_query":
            """
            SELECT
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            FROM
                '{}', '{}', '{}', '{}', '{}'
            WHERE
                p_partkey = ps_partkey
                AND s_suppkey = ps_suppkey
                AND p_size = 15
                AND p_type LIKE '%BRASS'
                AND s_nationkey = n_nationkey
                AND n_regionkey = r_regionkey
                AND r_name = 'EUROPE'
                AND ps_supplycost = (
                    SELECT
                        min(ps_supplycost)
                    FROM
                        '{}', '{}', '{}', '{}'
                    WHERE
                        p_partkey = ps_partkey
                        AND s_suppkey = ps_suppkey
                        AND s_nationkey = n_nationkey
                        AND n_regionkey = r_regionkey
                        AND r_name = 'EUROPE')
            ORDER BY
                s_acctbal DESC,
                n_name,
                s_name,
                p_partkey
            LIMIT 100;
            """,
        "substrait_query": get_substrait_plan('query_2_plan.json')
    }
]