from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer

PROJECT_RELATIONS = {
    "project_single_col": (
        """
        SELECT *
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "project_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "project_all_col": (
        """
        SELECT *
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "extended_project": (
        """
        SELECT L_QUANTITY, L_EXTENDEDPRICE*10 AS MULTI_PRICE
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "subquery_in_project": (
        """
        SELECT C_CUSTKEY,
           (SELECT SUM(O_TOTALPRICE) 
               FROM {}
               WHERE C_CUSTKEY = O_CUSTKEY) 
                 AS total_price
           FROM {}
           ORDER BY C_CUSTKEY
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "distinct_in_project": (
        """
        SELECT DISTINCT L_LINESTATUS
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "count_distinct_in_project": (
        """
        SELECT COUNT(DISTINCT L_EXTENDEDPRICE)
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer],
    ),

}
