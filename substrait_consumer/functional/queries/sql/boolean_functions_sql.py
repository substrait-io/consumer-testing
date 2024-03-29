from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

SQL_SCALAR = {
    "or": (
        """
        SELECT a
        FROM 't'
        WHERE a = 5 OR a = 7;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "and": (
        """
        SELECT a, b
        FROM 't'
        WHERE a < 5 AND b = 1;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "not": (
        """
        SELECT c FROM 't' WHERE NOT FALSE
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "xor": (
        """
        SELECT a, b,  xor(a, b) AS xor_a_b
        FROM 't';
        """,
        [DuckDBProducer],
    ),
}

SQL_AGGREGATE = {
    "bool_and": (
        """
        SELECT bool_and(c) AS bool_and_c
        FROM 't'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "bool_or": (
        """
        SELECT bool_or(c) AS bool_or_c
        FROM 't'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
}
