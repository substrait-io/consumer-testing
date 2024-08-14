from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

SQL_SCALAR = {
    "extract": (
        """
        SELECT L_SHIPDATE, extract(year FROM L_SHIPDATE)
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "add": (
        """
        SELECT L_SHIPDATE, L_SHIPDATE + INTERVAL 5 DAY
        FROM '{}'
        LIMIT 10;
        """,
        [DuckDBProducer],
    ),
    "subtract": (
        """
        SELECT L_SHIPDATE, L_SHIPDATE - INTERVAL 5 DAY
        FROM '{}'
        LIMIT 10;
        """,
        [DuckDBProducer],
    ),
    "lt": (
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE < L_RECEIPTDATE
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "lte": (
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE <= L_RECEIPTDATE
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "gt": (
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE > L_RECEIPTDATE
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "gte": (
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE >= L_RECEIPTDATE
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "add_intervals": (
        """
        SELECT INTERVAL 1 HOUR + INTERVAL 5 HOUR
        """,
        [DuckDBProducer],
    ),
}
