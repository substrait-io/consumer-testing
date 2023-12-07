from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer
SQL_SCALAR = {
    "ln": (
        """
        SELECT PS_SUPPLYCOST, round(ln(PS_SUPPLYCOST), 2) AS LN_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "log10": (
        """
        SELECT PS_SUPPLYCOST, round(log10(PS_SUPPLYCOST), 2) AS LOG10_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "log2": (
        """
        SELECT PS_SUPPLYCOST, round(log2(PS_SUPPLYCOST), 2) AS LOG2_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "logb": (
        """
        SELECT PS_SUPPLYCOST, round(logb(PS_SUPPLYCOST, 10), 2) AS LOGB_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
}
