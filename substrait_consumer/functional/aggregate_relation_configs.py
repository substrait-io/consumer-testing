from substrait_consumer.functional.queries.sql.relations.aggregate_relations import (
    AGGREGATE_RELATIONS)

AGGREGATE_RELATION_TESTS = (
    {
        "test_name": "single_measure_aggregate",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["single_measure_aggregate"],
    },
    {
        "test_name": "multiple_measure_aggregate",
        "local_files": {},
        "named_tables": {"orders": "orders_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["multiple_measure_aggregate"],
    },
    {
        "test_name": "aggregate_with_computation",
        "local_files": {},
        "named_tables": {"orders": "orders_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_computation"],
    },
    {
        "test_name": "compute_within_aggregate",
        "local_files": {},
        "named_tables": {"orders": "orders_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["compute_within_aggregate"],
    },
    {
        "test_name": "computation_between_aggregates",
        "local_files": {},
        "named_tables": {"orders": "orders_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["computation_between_aggregates"],
    },
    {
        "test_name": "aggregate_in_subquery",
        "local_files": {},
        "named_tables": {
            "orders": "orders_small.parquet",
            "orders": "orders_small.parquet",
        },
        "sql_query": AGGREGATE_RELATIONS["aggregate_in_subquery"],
    },
    {
        "test_name": "aggregate_with_group_by",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_group_by"],
    },
    {
        "test_name": "aggregate_with_group_by_cube",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_group_by_cube"],
    },
    {
        "test_name": "aggregate_with_group_by_rollup",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_group_by_rollup"],
    },
    {
        "test_name": "aggregate_with_grouping_set",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem_small.parquet"},
        "sql_query": AGGREGATE_RELATIONS["aggregate_with_grouping_set"],
    },
)
