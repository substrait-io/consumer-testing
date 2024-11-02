from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer
SQL_SCALAR = {
    "ceil": (
        """
        SELECT PS_SUPPLYCOST, ceil(CAST(PS_SUPPLYCOST AS DOUBLE)) AS CEIL_SUPPLYCOST
        FROM '{partsupp}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "floor": (
        """
        SELECT PS_SUPPLYCOST, floor(CAST(PS_SUPPLYCOST AS DOUBLE)) AS FLOOR_SUPPLYCOST
        FROM '{partsupp}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "round": (
        """
        SELECT L_EXTENDEDPRICE, round(CAST(L_EXTENDEDPRICE AS DOUBLE), 1) AS ROUND_EXTENDEDPRICE
        FROM '{lineitem}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
}
