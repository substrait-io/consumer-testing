from substrait_consumer.common import get_sql, get_substrait_plan

TPCH_QUERY_TESTS = (
    {
        "test_name": "test_tpch_sql_1",
        "file_names": ["lineitem.parquet"],
        "sql_query": get_sql("q1.sql"),
        "substrait_query": get_substrait_plan("query_01_plan.json"),
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
        "substrait_query": get_substrait_plan("query_02_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_3",
        "file_names": [
            "lineitem.parquet",
            "customer.parquet",
            "orders.parquet",
        ],
        "sql_query": get_sql("q3.sql"),
        "substrait_query": get_substrait_plan("query_03_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_4",
        "file_names": ["orders.parquet", "lineitem.parquet"],
        "sql_query": get_sql("q4.sql"),
        "substrait_query": get_substrait_plan("query_04_plan.json"),
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
        "substrait_query": get_substrait_plan("query_05_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_6",
        "file_names": ["lineitem.parquet"],
        "sql_query": get_sql("q6.sql"),
        "substrait_query": get_substrait_plan("query_06_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_7",
        "file_names": [
            "supplier.parquet",
            "lineitem.parquet",
            "orders.parquet",
            "customer.parquet",
            "nation.parquet",
            "nation.parquet",
        ],
        "sql_query": get_sql("q7.sql"),
        "substrait_query": get_substrait_plan("query_07_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_8",
        "file_names": [
            "part.parquet",
            "supplier.parquet",
            "lineitem.parquet",
            "orders.parquet",
            "customer.parquet",
            "nation.parquet",
            "nation.parquet",
            "region.parquet",
        ],
        "sql_query": get_sql("q8.sql"),
        "substrait_query": get_substrait_plan("query_08_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_9",
        "file_names": [
            "part.parquet",
            "supplier.parquet",
            "lineitem.parquet",
            "partsupp.parquet",
            "orders.parquet",
            "nation.parquet",
        ],
        "sql_query": get_sql("q9.sql"),
        "substrait_query": get_substrait_plan("query_09_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_10",
        "file_names": [
            "customer.parquet",
            "orders.parquet",
            "lineitem.parquet",
            "nation.parquet",
        ],
        "sql_query": get_sql("q10.sql"),
        "substrait_query": get_substrait_plan("query_10_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_11",
        "file_names": [
            "partsupp.parquet",
            "supplier.parquet",
            "nation.parquet",
            "partsupp.parquet",
            "supplier.parquet",
            "nation.parquet",
        ],
        "sql_query": get_sql("q11.sql"),
        "substrait_query": get_substrait_plan("query_11_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_12",
        "file_names": ["orders.parquet", "lineitem.parquet"],
        "sql_query": get_sql("q12.sql"),
        "substrait_query": get_substrait_plan("query_12_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_13",
        "file_names": ["customer.parquet", "orders.parquet"],
        "sql_query": get_sql("q13.sql"),
        "substrait_query": get_substrait_plan("query_13_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_14",
        "file_names": ["lineitem.parquet", "part.parquet"],
        "sql_query": get_sql("q14.sql"),
        "substrait_query": get_substrait_plan("query_14_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_15",
        "file_names": [
            "supplier.parquet",
            "lineitem.parquet",
            "lineitem.parquet",
        ],
        "sql_query": get_sql("q15.sql"),
        "substrait_query": get_substrait_plan("query_15_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_16",
        "file_names": ["partsupp.parquet", "part.parquet", "supplier.parquet"],
        "sql_query": get_sql("q16.sql"),
        "substrait_query": get_substrait_plan("query_16_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_17",
        "file_names": ["lineitem.parquet", "part.parquet", "lineitem.parquet"],
        "sql_query": get_sql("q17.sql"),
        "substrait_query": get_substrait_plan("query_17_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_18",
        "file_names": [
            "customer.parquet",
            "orders.parquet",
            "lineitem.parquet",
            "lineitem.parquet",
        ],
        "sql_query": get_sql("q18.sql"),
        "substrait_query": get_substrait_plan("query_18_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_19",
        "file_names": ["lineitem.parquet", "part.parquet"],
        "sql_query": get_sql("q19.sql"),
        "substrait_query": get_substrait_plan("query_19_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_20",
        "file_names": [
            "supplier.parquet",
            "nation.parquet",
            "partsupp.parquet",
            "part.parquet",
            "lineitem.parquet",
        ],
        "sql_query": get_sql("q20.sql"),
        "substrait_query": get_substrait_plan("query_20_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_21",
        "file_names": [
            "supplier.parquet",
            "lineitem.parquet",
            "orders.parquet",
            "nation.parquet",
            "lineitem.parquet",
            "lineitem.parquet",
        ],
        "sql_query": get_sql("q21.sql"),
        "substrait_query": get_substrait_plan("query_21_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_22",
        "file_names": ["customer.parquet", "customer.parquet", "orders.parquet"],
        "sql_query": get_sql("q22.sql"),
        "substrait_query": get_substrait_plan("query_22_plan.json"),
    },
)
