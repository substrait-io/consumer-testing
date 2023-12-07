from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer

SQL_AGGREGATE = {
    "approx_count_distinct": (
        """
        SELECT approx_count_distinct(l_comment)
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "approx_distinct": (
        """
        SELECT approx_distinct(l_comment)
        FROM '{}';
        """,
        [DataFusionProducer],
    ),
}
