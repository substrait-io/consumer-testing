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
    "aggregate_with_grouping_set": (
        """
    
        SELECT SUM(l_extendedprice), l_linenumber
        FROM '{}'
        GROUP BY l_linenumber
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
