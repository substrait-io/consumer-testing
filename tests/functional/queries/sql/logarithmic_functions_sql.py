from tests.producers import *

SQL_SCALAR = {
    "ln": (
        """
        SELECT PS_SUPPLYCOST, ln(PS_SUPPLYCOST) AS LN_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "log10": (
        """
        SELECT PS_SUPPLYCOST, log10(PS_SUPPLYCOST) AS LOG10_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "log2": (
        """
        SELECT PS_SUPPLYCOST, log2(PS_SUPPLYCOST) AS LOG2_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "logb": (
        """
        SELECT PS_SUPPLYCOST, logb(PS_SUPPLYCOST, 10) AS LOGB_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
}
