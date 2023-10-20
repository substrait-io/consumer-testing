from substrait_consumer.producers import *

SQL_SCALAR = {
    "ln": (
        """
        SELECT PS_SUPPLYCOST, round(ln(PS_SUPPLYCOST), 2) AS LN_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DuckDBProducer],
    ),
    "log10": (
        """
        SELECT PS_SUPPLYCOST, round(log10(PS_SUPPLYCOST), 2) AS LOG10_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DuckDBProducer],
    ),
    "log2": (
        """
        SELECT PS_SUPPLYCOST, round(log2(PS_SUPPLYCOST), 2) AS LOG2_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DuckDBProducer],
    ),
    "logb": (
        """
        SELECT PS_SUPPLYCOST, round(logb(PS_SUPPLYCOST, 10), 2) AS LOGB_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DuckDBProducer],
    ),
}
