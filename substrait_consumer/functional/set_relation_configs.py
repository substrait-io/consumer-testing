from substrait_consumer.functional.queries.sql.relations.set_relations import (
    SET_RELATIONS)

SET_RELATION_TESTS = (
    {
        "test_name": "union_distinct",
        "file_names": ["customer_small.parquet", "nation_small.parquet"],
        "sql_query": SET_RELATIONS["union_distinct"],
        "ibis_expr": None
    },
    {
        "test_name": "union_all",
        "file_names": ["customer_small.parquet", "nation_small.parquet"],
        "sql_query": SET_RELATIONS["union_all"],
        "ibis_expr": None
    },
    {
        "test_name": "intersect",
        "file_names": ["customer_small.parquet", "nation_small.parquet"],
        "sql_query": SET_RELATIONS["intersect"],
        "ibis_expr": None
    },
    {
        "test_name": "except",
        "file_names": ["orders_small.parquet", "customer_small.parquet"],
        "sql_query": SET_RELATIONS["except"],
        "ibis_expr": None
    },
)
