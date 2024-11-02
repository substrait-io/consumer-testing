from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

SET_RELATIONS = {
    "union_distinct": (
        """
        SELECT C_NATIONKEY FROM '{customer}'
        UNION
        SELECT N_NATIONKEY FROM '{nation}'
        ORDER BY C_NATIONKEY
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "union_all": (
        """
        SELECT C_NATIONKEY FROM '{customer}'
        UNION ALL
        SELECT N_NATIONKEY FROM '{nation}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "intersect": (
        """
        SELECT C_NATIONKEY FROM '{customer}'
        INTERSECT
        SELECT N_NATIONKEY FROM '{nation}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "except": (
        """
        SELECT o_totalprice FROM '{orders}'
        EXCEPT
        SELECT c_acctbal FROM '{customer}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
