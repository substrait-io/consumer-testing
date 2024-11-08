from substrait_consumer.functional.queries.sql.relations.join_relations import (
    JOIN_RELATIONS)

JOIN_RELATION_TESTS = (
    {
        "test_name": "inner_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["inner_join"],
        "ibis_expr": None
    },
    {
        "test_name": "left_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["left_join"],
        "ibis_expr": None
    },
    {
        "test_name": "right_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["right_join"],
        "ibis_expr": None
    },
    {
        "test_name": "full_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["full_join"],
        "ibis_expr": None
    },
    {
        "test_name": "cross_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["cross_join"],
        "ibis_expr": None
    },
    {
        "test_name": "left_semi_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["left_semi_join"],
        "ibis_expr": None
    },
    {
        "test_name": "right_semi_join",
        "local_files": {},
        "named_tables": {
            "orders": "orders_small.parquet",
            "customer": "customer_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["right_semi_join"],
        "ibis_expr": None
    },
    {
        "test_name": "left_anti_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["left_anti_join"],
        "ibis_expr": None
    },
    {
        "test_name": "right_anti_join",
        "local_files": {},
        "named_tables": {
            "orders": "orders_small.parquet",
            "lineitem": "lineitem_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["right_anti_join"],
        "ibis_expr": None
    },
    {
        "test_name": "left_single_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "customer": "customer_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["left_single_join"],
        "ibis_expr": None
    },
    {
        "test_name": "right_single_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "customer": "customer_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["right_single_join"],
        "ibis_expr": None
    },
    {
        "test_name": "left_mark_join",
        "local_files": {},
        "named_tables": {
            "orders": "orders_small.parquet",
            "customer": "customer_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["left_mark_join"],
        "ibis_expr": None
    },
    {
        "test_name": "right_mark_join",
        "local_files": {},
        "named_tables": {
            "customer": "customer_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": JOIN_RELATIONS["right_mark_join"],
        "ibis_expr": None
    },
)
