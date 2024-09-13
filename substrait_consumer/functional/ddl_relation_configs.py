from substrait_consumer.functional.queries.sql.relations.ddl_relations import (
    DDL_RELATIONS)

DDL_RELATION_TESTS = (
    {
        "test_name": "create_table",
        "file_names": [],
        "sql_query": DDL_RELATIONS["create_table"],
        "ibis_expr": None
    },
    {
        "test_name": "drop_table",
        "file_names": ["customer.parquet"],
        "sql_query": DDL_RELATIONS["drop_table"],
        "ibis_expr": None
    },
    {
        "test_name": "alter_table",
        "file_names": ["customer.parquet"],
        "sql_query": DDL_RELATIONS["alter_table"],
        "ibis_expr": None
    },
    {
        "test_name": "alter_column",
        "file_names": ["customer.parquet"],
        "sql_query": DDL_RELATIONS["alter_column"],
        "ibis_expr": None
    },
    {
        "test_name": "drop_column",
        "file_names": ["customer.parquet"],
        "sql_query": DDL_RELATIONS["drop_column"],
        "ibis_expr": None
    },
    {
        "test_name": "create_view",
        "file_names": ["customer.parquet"],
        "sql_query": DDL_RELATIONS["create_view"],
        "ibis_expr": None
    },
    {
        "test_name": "create_or_replace_view",
        "file_names": ["customer.parquet"],
        "sql_query": DDL_RELATIONS["create_or_replace_view"],
        "ibis_expr": None
    },
)
