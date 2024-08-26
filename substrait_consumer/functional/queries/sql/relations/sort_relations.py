from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

SORT_RELATIONS = {
    "single_col_default_sort": (
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_AVAILQTY
        LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "single_col_asc": (
        """
        SELECT PS_SUPPLYCOST
        FROM '{}'
        ORDER BY PS_SUPPLYCOST ASC
        LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "single_col_desc": (
        """
        SELECT PS_SUPPLYCOST
        FROM '{}'
        ORDER BY PS_SUPPLYCOST DESC
        LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "multi_col_asc": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_SUPPLYCOST ASC, PS_AVAILQTY
        LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "multi_col_desc": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_SUPPLYCOST DESC
        LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "multi_col_asc_desc": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_SUPPLYCOST ASC, PS_AVAILQTY DESC
        LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "multi_col_desc_asc": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_SUPPLYCOST DESC, PS_AVAILQTY ASC
        LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "order_by_col_number": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY 1, 2
        LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
