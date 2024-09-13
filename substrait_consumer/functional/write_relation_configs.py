from substrait_consumer.functional.queries.sql.relations.write_relations import (
    WRITE_RELATIONS)

WRITE_RELATION_TESTS = (
    {
        "test_name": "insert",
        "file_names": ["region.parquet"],
        "sql_query": WRITE_RELATIONS["insert"],
        "ibis_expr": None
    },
    {
        "test_name": "update",
        "file_names": ["customer.parquet"],
        "sql_query": WRITE_RELATIONS["update"],
        "ibis_expr": None
    },
    {
        "test_name": "delete",
        "file_names": ["customer.parquet"],
        "sql_query": WRITE_RELATIONS["delete"],
        "ibis_expr": None
    },
)
