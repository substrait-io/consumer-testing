from substrait_consumer.functional.queries.sql.relations.set_relations import (
    SET_RELATIONS)

SET_RELATION_TESTS = (
    {
        "test_name": "union_distinct",
        "local_files": {},
        "named_tables": {"customer": "customer_small.parquet", "nation": "nation_small.parquet"},
        "sql_query": SET_RELATIONS["union_distinct"],
        "ibis_expr": None
    },
    {
        "test_name": "union_all",
        "local_files": {},
        "named_tables": {"customer": "customer_small.parquet", "nation": "nation_small.parquet"},
        "sql_query": SET_RELATIONS["union_all"],
        "ibis_expr": None
    },
    {
        "test_name": "intersect",
        "local_files": {},
        "named_tables": {"customer": "customer_small.parquet", "nation": "nation_small.parquet"},
        "sql_query": SET_RELATIONS["intersect"],
        "ibis_expr": None
    },
    {
        "test_name": "except",
        "local_files": {},
        "named_tables": {"orders": "orders_small.parquet", "customer": "customer_small.parquet"},
        "sql_query": SET_RELATIONS["except"],
        "ibis_expr": None
    },
)
