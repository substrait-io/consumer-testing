from substrait_consumer.functional.queries.sql.relations.project_relations import (
    PROJECT_RELATIONS)

PROJECT_RELATION_TESTS = (
    {
        "test_name": "project_single_col",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": PROJECT_RELATIONS["project_single_col"],
        "ibis_expr": None
    },
    {
        "test_name": "project_multi_col",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": PROJECT_RELATIONS["project_multi_col"],
        "ibis_expr": None
    },
    {
        "test_name": "project_all_col",
        "local_files": {},
        "named_tables": {"region": "region_small.parquet"},
        "sql_query": PROJECT_RELATIONS["project_all_col"],
        "ibis_expr": None
    },
    {
        "test_name": "extended_project",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": PROJECT_RELATIONS["extended_project"],
        "ibis_expr": None
    },
    {
        "test_name": "subquery_in_project",
        "local_files": {},
        "named_tables": {
            "orders": "orders_small.parquet",
            "customer": "customer_small.parquet",
        },
        "sql_query": PROJECT_RELATIONS["subquery_in_project"],
        "ibis_expr": None
    },
    {
        "test_name": "distinct_in_project",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": PROJECT_RELATIONS["distinct_in_project"],
        "ibis_expr": None
    },
    {
        "test_name": "count_distinct_in_project",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": PROJECT_RELATIONS["count_distinct_in_project"],
        "ibis_expr": None
    },
)
