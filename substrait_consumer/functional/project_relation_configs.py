from substrait_consumer.functional.queries.sql.relations.project_relations import (
    PROJECT_RELATIONS)

PROJECT_RELATION_TESTS = (
    {
        "test_name": "project_single_col",
        "file_names": ["lineitem_small.parquet"],
        "sql_query": PROJECT_RELATIONS["project_single_col"],
        "ibis_expr": None
    },
    {
        "test_name": "project_multi_col",
        "file_names": ["lineitem_small.parquet"],
        "sql_query": PROJECT_RELATIONS["project_multi_col"],
        "ibis_expr": None
    },
    {
        "test_name": "project_all_col",
        "file_names": ["region_small.parquet"],
        "sql_query": PROJECT_RELATIONS["project_all_col"],
        "ibis_expr": None
    },
    {
        "test_name": "extended_project",
        "file_names": ["lineitem_small.parquet"],
        "sql_query": PROJECT_RELATIONS["extended_project"],
        "ibis_expr": None
    },
    {
        "test_name": "subquery_in_project",
        "file_names": ["orders_small.parquet", "customer_small.parquet"],
        "sql_query": PROJECT_RELATIONS["subquery_in_project"],
        "ibis_expr": None
    },
    {
        "test_name": "distinct_in_project",
        "file_names": ["lineitem_small.parquet"],
        "sql_query": PROJECT_RELATIONS["distinct_in_project"],
        "ibis_expr": None
    },
    {
        "test_name": "count_distinct_in_project",
        "file_names": ["lineitem_small.parquet"],
        "sql_query": PROJECT_RELATIONS["count_distinct_in_project"],
        "ibis_expr": None
    },
)
