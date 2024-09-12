from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

WRITE_RELATIONS = {
    "insert": (
        """
        INSERT INTO '{}' (r_regionkey, r_name, r_comment)
        VALUES (99999, 'region_name', 'region comment');
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "update": (
        """
        UPDATE '{}'
        SET c_address = 'Substait Avenue', c_phone = '123-456-7890'
        WHERE c_custkey = 1;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "delete": (
        """
        DELETE FROM '{}'
        WHERE c_custkey = 1;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
