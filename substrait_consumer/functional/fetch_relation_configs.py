from substrait_consumer.functional.queries.sql.relations.fetch_relations import (
    FETCH_RELATIONS)

FETCH_RELATION_TESTS = (
    {
        "test_name": "fetch",
        "local_files": {},
        "named_tables": {"orders": "orders.parquet"},
        "sql_query": FETCH_RELATIONS["fetch"],
    },
    {
        "test_name": "fetch_with_offset",
        "local_files": {},
        "named_tables": {"orders": "orders.parquet"},
        "sql_query": FETCH_RELATIONS["fetch_with_offset"],
    },
)
