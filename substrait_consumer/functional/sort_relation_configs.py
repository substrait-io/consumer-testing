from substrait_consumer.functional.queries.sql.relations.sort_relations import (
    SORT_RELATIONS)

SORT_RELATION_TESTS = (
    {
        "test_name": "single_col_default_sort",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SORT_RELATIONS["single_col_default_sort"],
        "ibis_expr": None
    },
    {
        "test_name": "single_col_asc",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SORT_RELATIONS["single_col_asc"],
        "ibis_expr": None
    },
    {
        "test_name": "single_col_desc",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SORT_RELATIONS["single_col_desc"],
        "ibis_expr": None
    },
    {
        "test_name": "multi_col_asc",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SORT_RELATIONS["multi_col_asc"],
        "ibis_expr": None
    },
    {
        "test_name": "multi_col_desc",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SORT_RELATIONS["single_col_desc"],
        "ibis_expr": None
    },
    {
        "test_name": "multi_col_asc_desc",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SORT_RELATIONS["multi_col_asc_desc"],
        "ibis_expr": None
    },
    {
        "test_name": "multi_col_desc_asc",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SORT_RELATIONS["multi_col_desc_asc"],
        "ibis_expr": None
    },
    {
        "test_name": "order_by_col_number",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SORT_RELATIONS["order_by_col_number"],
        "ibis_expr": None
    },
)
