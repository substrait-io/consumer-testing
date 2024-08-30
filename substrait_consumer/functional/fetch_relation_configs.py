from substrait_consumer.functional.queries.sql.relations.fetch_relations import (
    FETCH_RELATIONS)

FETCH_RELATION_TESTS = (
    {
        "test_name": "fetch",
        "file_names": ["orders.parquet"],
        "sql_query": FETCH_RELATIONS["fetch"],
        "ibis_expr": None
    },
    {
        "test_name": "fetch_with_offset",
        "file_names": ["orders.parquet"],
        "sql_query": FETCH_RELATIONS["fetch_with_offset"],
        "ibis_expr": None
    },
)
