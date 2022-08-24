from ...common import get_sql, get_substrait_plan

TPCH_QUERY_TESTS = (
    {
        "test_name": "test_tpch_sql_1",
        "file_names": ["lineitem.parquet"],
        "sql_query": get_sql("q1.sql"),
        "substrait_query": get_substrait_plan("query_1_plan.json"),
    },
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
    },
    {
        "test_name": "test_tpch_sql_3",
        "file_names": [
            "lineitem.parquet",
            "customer.parquet",
            "orders.parquet",
        ],
        "sql_query": get_sql("q3.sql"),
        "substrait_query": get_substrait_plan("query_3_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_4",
        "file_names": ["orders.parquet", "lineitem.parquet"],
        "sql_query": get_sql("q4.sql"),
        "substrait_query": get_substrait_plan("query_4_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_5",
        "file_names": [
            "customer.parquet",
            "orders.parquet",
            "lineitem.parquet",
            "supplier.parquet",
            "nation.parquet",
            "region.parquet",
        ],
        "sql_query": get_sql("q5.sql"),
        "substrait_query": get_substrait_plan("query_5_plan.json"),
    },
)
