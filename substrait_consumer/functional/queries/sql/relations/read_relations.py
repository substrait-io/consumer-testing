from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

READ_RELATIONS = {
    "read_named_table": (
        """
        SELECT PS_PARTKEY FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "isthmus_read_virtual_table": (
        """
        SELECT 10
        """,
        [IsthmusProducer],
    ),
    "datafusion_read_virtual_table": (
        """
        CREATE TABLE IF NOT EXISTS valuetable AS VALUES(1,'HELLO'),(12,'DATAFUSION');
        """,
        [DataFusionProducer],
    ),
    "duckdb_read_virtual_table": (
        """
        CREATE TABLE IF NOT EXISTS t1 (i INTEGER, j INTEGER);
        """,
        [DuckDBProducer],
    ),
    "duckdb_read_local_file": (
        """
        SELECT * FROM read_parquet('{}');
        """,
        [DuckDBProducer],
    ),
}
