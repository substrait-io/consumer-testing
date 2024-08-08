from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer

SORT_RELATIONS = {
    "single_col_default_sort": (
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_AVAILQTY;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "single_col_asc": (
        """
        SELECT PS_SUPPLYCOST
        FROM '{}'
        ORDER BY PS_SUPPLYCOST ASC;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "single_col_desc": (
        """
        SELECT PS_SUPPLYCOST
        FROM '{}'
        ORDER BY PS_SUPPLYCOST DESC;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "multi_col_asc": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_SUPPLYCOST ASC;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "multi_col_desc": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_SUPPLYCOST DESC;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "multi_col_asc_desc": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_SUPPLYCOST ASC, PS_AVAILQTY DESC;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "multi_col_desc_asc": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY PS_SUPPLYCOST DESC, PS_AVAILQTY ASC;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "order_by_col_number": (
        """
        SELECT PS_SUPPLYCOST, PS_AVAILQTY
        FROM '{}'
        ORDER BY 1;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
}
