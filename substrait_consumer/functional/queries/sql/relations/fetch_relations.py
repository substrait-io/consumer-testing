from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

FETCH_RELATIONS = {
    "fetch": (
        """
        SELECT O_ORDERKEY FROM '{}'
        FETCH NEXT 1 ROWS ONLY;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "fetch_with_offset": (
        """
        SELECT O_ORDERKEY FROM '{}'
        OFFSET 5 ROWS
        FETCH NEXT 5 ROWS ONLY;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),

}
