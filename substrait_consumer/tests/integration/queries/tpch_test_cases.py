import json
from pathlib import Path

from substrait_consumer.common import get_sql, get_substrait_plan

TPCH_QUERY_TESTS = (
    {
        "test_name": "test_tpch_sql_1",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": get_sql("q1.sql"),
        "substrait_query": get_substrait_plan("query_01_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_2",
        "local_files": {},
        "named_tables": {
            "part": "part.parquet",
            "supplier": "supplier.parquet",
            "partsupp": "partsupp.parquet",
            "nation": "nation.parquet",
            "region": "region.parquet",
            "partsupp": "partsupp.parquet",
            "supplier": "supplier.parquet",
            "nation": "nation.parquet",
            "region": "region.parquet",
        },
        "sql_query": get_sql("q2.sql"),
        "substrait_query": get_substrait_plan("query_02_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_3",
        "local_files": {},
        "named_tables": {
            "lineitem": "lineitem.parquet",
            "customer": "customer.parquet",
            "orders": "orders.parquet",
        },
        "sql_query": get_sql("q3.sql"),
        "substrait_query": get_substrait_plan("query_03_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_4",
        "local_files": {},
        "named_tables": {"orders": "orders.parquet", "lineitem": "lineitem.parquet"},
        "sql_query": get_sql("q4.sql"),
        "substrait_query": get_substrait_plan("query_04_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_5",
        "local_files": {},
        "named_tables": {
            "customer": "customer.parquet",
            "orders": "orders.parquet",
            "lineitem": "lineitem.parquet",
            "supplier": "supplier.parquet",
            "nation": "nation.parquet",
            "region": "region.parquet",
        },
        "sql_query": get_sql("q5.sql"),
        "substrait_query": get_substrait_plan("query_05_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_6",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": get_sql("q6.sql"),
        "substrait_query": get_substrait_plan("query_06_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_7",
        "local_files": {},
        "named_tables": {
            "supplier": "supplier.parquet",
            "lineitem": "lineitem.parquet",
            "orders": "orders.parquet",
            "customer": "customer.parquet",
            "nation": "nation.parquet",
            "nation": "nation.parquet",
        },
        "sql_query": get_sql("q7.sql"),
        "substrait_query": get_substrait_plan("query_07_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_8",
        "local_files": {},
        "named_tables": {
            "part": "part.parquet",
            "supplier": "supplier.parquet",
            "lineitem": "lineitem.parquet",
            "orders": "orders.parquet",
            "customer": "customer.parquet",
            "nation": "nation.parquet",
            "nation": "nation.parquet",
            "region": "region.parquet",
        },
        "sql_query": get_sql("q8.sql"),
        "substrait_query": get_substrait_plan("query_08_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_9",
        "local_files": {},
        "named_tables": {
            "part": "part.parquet",
            "supplier": "supplier.parquet",
            "lineitem": "lineitem.parquet",
            "partsupp": "partsupp.parquet",
            "orders": "orders.parquet",
            "nation": "nation.parquet",
        },
        "sql_query": get_sql("q9.sql"),
        "substrait_query": get_substrait_plan("query_09_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_10",
        "local_files": {},
        "named_tables": {
            "customer": "customer.parquet",
            "orders": "orders.parquet",
            "lineitem": "lineitem.parquet",
            "nation": "nation.parquet",
        },
        "sql_query": get_sql("q10.sql"),
        "substrait_query": get_substrait_plan("query_10_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_11",
        "local_files": {},
        "named_tables": {
            "partsupp": "partsupp.parquet",
            "supplier": "supplier.parquet",
            "nation": "nation.parquet",
            "partsupp": "partsupp.parquet",
            "supplier": "supplier.parquet",
            "nation": "nation.parquet",
        },
        "sql_query": get_sql("q11.sql"),
        "substrait_query": get_substrait_plan("query_11_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_12",
        "local_files": {},
        "named_tables": {"orders": "orders.parquet", "lineitem": "lineitem.parquet"},
        "sql_query": get_sql("q12.sql"),
        "substrait_query": get_substrait_plan("query_12_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_13",
        "local_files": {},
        "named_tables": {"customer": "customer.parquet", "orders": "orders.parquet"},
        "sql_query": get_sql("q13.sql"),
        "substrait_query": get_substrait_plan("query_13_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_14",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet", "part": "part.parquet"},
        "sql_query": get_sql("q14.sql"),
        "substrait_query": get_substrait_plan("query_14_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_15",
        "local_files": {},
        "named_tables": {
            "supplier": "supplier.parquet",
            "lineitem": "lineitem.parquet",
            "lineitem": "lineitem.parquet",
        },
        "sql_query": get_sql("q15.sql"),
        "substrait_query": get_substrait_plan("query_15_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_16",
        "local_files": {},
        "named_tables": {
            "partsupp": "partsupp.parquet",
            "part": "part.parquet",
            "supplier": "supplier.parquet",
        },
        "sql_query": get_sql("q16.sql"),
        "substrait_query": get_substrait_plan("query_16_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_17",
        "local_files": {},
        "named_tables": {
            "lineitem": "lineitem.parquet",
            "part": "part.parquet",
            "lineitem": "lineitem.parquet",
        },
        "sql_query": get_sql("q17.sql"),
        "substrait_query": get_substrait_plan("query_17_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_18",
        "local_files": {},
        "named_tables": {
            "customer": "customer.parquet",
            "orders": "orders.parquet",
            "lineitem": "lineitem.parquet",
            "lineitem": "lineitem.parquet",
        },
        "sql_query": get_sql("q18.sql"),
        "substrait_query": get_substrait_plan("query_18_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_19",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet", "part": "part.parquet"},
        "sql_query": get_sql("q19.sql"),
        "substrait_query": get_substrait_plan("query_19_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_20",
        "local_files": {},
        "named_tables": {
            "supplier": "supplier.parquet",
            "nation": "nation.parquet",
            "partsupp": "partsupp.parquet",
            "part": "part.parquet",
            "lineitem": "lineitem.parquet",
        },
        "sql_query": get_sql("q20.sql"),
        "substrait_query": get_substrait_plan("query_20_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_21",
        "local_files": {},
        "named_tables": {
            "supplier": "supplier.parquet",
            "lineitem": "lineitem.parquet",
            "orders": "orders.parquet",
            "nation": "nation.parquet",
            "lineitem": "lineitem.parquet",
            "lineitem": "lineitem.parquet",
        },
        "sql_query": get_sql("q21.sql"),
        "substrait_query": get_substrait_plan("query_21_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_22",
        "local_files": {},
        "named_tables": {
            "customer": "customer.parquet",
            "customer": "customer.parquet",
            "orders": "orders.parquet",
        },
        "sql_query": get_sql("q22.sql"),
        "substrait_query": get_substrait_plan("query_22_plan.json"),
    },
)

BASE_PATH = Path(__file__).parent.parent / "tpch"

for test in TPCH_QUERY_TESTS:
    sql_query = test.pop("sql_query")
    test.pop("substrait_query")
    query_num = int(test["test_name"].split("_")[3])
    short_name = f"q{query_num:02d}"
    long_name = f"tpch_query_{query_num:02d}"
    test["sql_query"] = {
        "producers": ["duckdb", "isthmus"],
        "query_path": f"{short_name}.sql",
    }
    test["test_name"] = long_name

    test_path = BASE_PATH / f"{short_name}.json"
    sql_path = BASE_PATH / f"{short_name}.sql"

    with open(sql_path, "w") as f:
        f.write(sql_query)

    with open(test_path, "w") as f:
        json.dump(test, f, indent=4, sort_keys=True)
