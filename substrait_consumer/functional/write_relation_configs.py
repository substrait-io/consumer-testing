from substrait_consumer.functional.queries.sql.relations.write_relations import (
    WRITE_RELATIONS)

WRITE_RELATION_TESTS = (
    {
        "test_name": "insert",
        "local_files": {},
        "named_tables": {"region": "region.parquet"},
        "sql_query": WRITE_RELATIONS["insert"],
        "ibis_expr": None
    },
    {
        "test_name": "update",
        "local_files": {},
        "named_tables": {"customer": "customer.parquet"},
        "sql_query": WRITE_RELATIONS["update"],
        "ibis_expr": None
    },
    {
        "test_name": "delete",
        "local_files": {},
        "named_tables": {"customer": "customer.parquet"},
        "sql_query": WRITE_RELATIONS["delete"],
        "ibis_expr": None
    },
)
