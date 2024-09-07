from substrait_consumer.functional.queries.sql.relations.read_relations import (
    READ_RELATIONS)

READ_RELATION_TESTS = (
    {
        "test_name": "read_named_table",
        "file_names": ['partsupp_small.parquet'],
        "sql_query": READ_RELATIONS["read_named_table"],
        "ibis_expr": None
    },
    {
        "test_name": "isthmus_read_virtual_table",
        "file_names": [],
        "sql_query": READ_RELATIONS["isthmus_read_virtual_table"],
        "ibis_expr": None
    },
    {
        "test_name": "datafusion_read_virtual_table",
        "file_names": [],
        "sql_query": READ_RELATIONS["datafusion_read_virtual_table"],
        "ibis_expr": None
    },
    {
        "test_name": "duckdb_read_virtual_table",
        "file_names": [],
        "sql_query": READ_RELATIONS["duckdb_read_virtual_table"],
        "ibis_expr": None
    },
    {
        "test_name": "duckdb_read_local_file",
        "file_names": ['customer_small.parquet'],
        "sql_query": READ_RELATIONS["duckdb_read_local_file"],
        "ibis_expr": None
    },
)
