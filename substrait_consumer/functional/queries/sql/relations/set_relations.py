from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

SET_RELATIONS = {
    "union_distinct": (
        """
        SELECT C_NATIONKEY FROM '{}'
        UNION
        SELECT N_NATIONKEY FROM '{}'
        ORDER BY C_NATIONKEY
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "union_all": (
        """
        SELECT C_NATIONKEY FROM '{}'
        UNION ALL
        SELECT N_NATIONKEY FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "intersect": (
        """
        SELECT C_NATIONKEY FROM '{}'
        INTERSECT
        SELECT N_NATIONKEY FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "except": (
        """
        SELECT o_totalprice FROM '{}'
        EXCEPT
        SELECT c_acctbal FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
