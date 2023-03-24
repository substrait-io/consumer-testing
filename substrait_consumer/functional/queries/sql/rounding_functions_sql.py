from substrait_consumer.producers import *

SQL_SCALAR = {
    "ceil": (
        """
        SELECT PS_SUPPLYCOST, ceil(PS_SUPPLYCOST) AS CEIL_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "floor": (
        """
        SELECT PS_SUPPLYCOST, floor(PS_SUPPLYCOST) AS FLOOR_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "round": (
        """
        SELECT L_EXTENDEDPRICE, round(L_EXTENDEDPRICE, 1) AS ROUND_EXTENDEDPRICE
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
}
