from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

AGGREGATE_RELATIONS = {
    "single_measure_aggregate": (
        """
        SELECT COUNT(L_PARTKEY)
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "multiple_measure_aggregate": (
        """
        SELECT MIN(O_TOTALPRICE), MAX(O_TOTALPRICE), AVG(O_TOTALPRICE)
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "aggregate_with_computation": (
        """
        SELECT AVG(O_TOTALPRICE) * 10
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "compute_within_aggregate": (
        """
        SELECT AVG(O_TOTALPRICE * 10)
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "computation_between_aggregates": (
        """
        SELECT AVG(O_TOTALPRICE) + MAX(O_TOTALPRICE)
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "aggregate_in_subquery": (
        """
        
        SELECT O_TOTALPRICE
        FROM '{}'
        WHERE O_TOTALPRICE <= (SELECT AVG(O_TOTALPRICE) FROM '{}')
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "aggregate_with_group_by": (
        """
        SELECT L_ORDERKEY, L_LINENUMBER, count(*)
        FROM '{}'
        GROUP BY L_ORDERKEY, L_LINENUMBER
        ORDER BY L_ORDERKEY, L_LINENUMBER
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "aggregate_with_group_by_cube": (
        """
        SELECT L_ORDERKEY, L_LINENUMBER, count(*)
        FROM '{}'
        GROUP BY CUBE(L_ORDERKEY, L_LINENUMBER)
        ORDER BY L_ORDERKEY, L_LINENUMBER
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "aggregate_with_group_by_rollup": (
        """
    
        SELECT L_ORDERKEY, L_LINENUMBER, count(*)
        FROM '{}'
        GROUP BY ROLLUP(L_ORDERKEY, L_LINENUMBER)
        ORDER BY L_ORDERKEY, L_LINENUMBER
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "aggregate_with_grouping_set": (
        """
    
        SELECT SUM(L_EXTENDEDPRICE), L_LINENUMBER, L_ORDERKEY
        FROM '{}'
        GROUP BY GROUPING SETS 
        (
        (L_LINENUMBER),
        (L_ORDERKEY)
        )
        ORDER BY L_LINENUMBER
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
