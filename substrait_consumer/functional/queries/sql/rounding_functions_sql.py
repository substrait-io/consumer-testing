from substrait_consumer.producers import *

SQL_SCALAR = {
    "ceil": (
        """
        SELECT ceil(PS_SUPPLYCOST) AS CEIL_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "floor": (
        """
        SELECT floor(PS_SUPPLYCOST) AS FLOOR_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
}
