from substrait_consumer.functional.queries.sql.relations.aggregate_relations import (
    AGGREGATE_RELATIONS)

AGGREGATE_RELATION_TESTS = (
    {
        "test_name": "single_measure_aggregate",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["single_measure_aggregate"],
        "ibis_expr": None
    },
    {
        "test_name": "multiple_measure_aggregate",
        "local_files": {},
        "named_tables": {"orders": "orders_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["multiple_measure_aggregate"],
        "ibis_expr": None
    },
    {
        "test_name": "aggregate_with_computation",
        "local_files": {},
        "named_tables": {"orders": "orders_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_computation"],
        "ibis_expr": None
    },
    {
        "test_name": "compute_within_aggregate",
        "local_files": {},
        "named_tables": {"orders": "orders_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["compute_within_aggregate"],
        "ibis_expr": None
    },
    {
        "test_name": "computation_between_aggregates",
        "local_files": {},
        "named_tables": {"orders": "orders_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["computation_between_aggregates"],
        "ibis_expr": None
    },
    {
        "test_name": "aggregate_in_subquery",
        "local_files": {},
        "named_tables": {
            "orders": "orders_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": AGGREGATE_RELATIONS["aggregate_in_subquery"],
        "ibis_expr": None
    },
    {
        "test_name": "aggregate_with_group_by",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_group_by"],
        "ibis_expr": None
    },
    {
        "test_name": "aggregate_with_group_by_cube",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_group_by_cube"],
        "ibis_expr": None
    },
    {
        "test_name": "aggregate_with_group_by_rollup",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_group_by_rollup"],
        "ibis_expr": None
    },
    {
        "test_name": "aggregate_with_grouping_set",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_grouping_set"],
        "ibis_expr": None
    },
)
