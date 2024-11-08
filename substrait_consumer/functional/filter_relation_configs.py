from substrait_consumer.functional.queries.sql.relations.filter_relations import (
    FILTER_RELATIONS)

FILTER_RELATION_TESTS = (
    {
        "test_name": "where_equal_multi_col",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_equal_multi_col"],
        "ibis_expr": None
    },
    {
        "test_name": "where_not_equal_multi_col",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_not_equal_multi_col"],
        "ibis_expr": None
    },
    {
        "test_name": "where_gt_multi_col",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_gt_multi_col"],
        "ibis_expr": None
    },
    {
        "test_name": "where_gte_multi_col",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_gte_multi_col"],
        "ibis_expr": None
    },
    {
        "test_name": "where_lt_multi_col",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_lt_multi_col"],
        "ibis_expr": None
    },
    {
        "test_name": "where_lte_multi_col",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_lte_multi_col"],
        "ibis_expr": None
    },
    {
        "test_name": "where_like",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_like"],
        "ibis_expr": None
    },
    {
        "test_name": "where_between",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_between"],
        "ibis_expr": None
    },
    {
        "test_name": "where_in",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_in"],
        "ibis_expr": None
    },
    {
        "test_name": "where_or",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_or"],
        "ibis_expr": None
    },
    {
        "test_name": "where_and",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["where_and"],
        "ibis_expr": None
    },
    {
        "test_name": "having",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": FILTER_RELATIONS["having"],
        "ibis_expr": None
    },
)
