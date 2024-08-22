from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer

PROJECT_RELATIONS = {
    "project_single_col": (
        """
        SELECT L_DISCOUNT
        FROM '{}'
        ORDER BY L_DISCOUNT
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "project_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{}'
        ORDER BY L_DISCOUNT, L_TAX
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "project_all_col": (
        """
        SELECT *
        FROM '{}'
        ORDER BY R_REGIONKEY, R_NAME, R_COMMENT
        LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "extended_project": (
        """
        SELECT L_QUANTITY, L_EXTENDEDPRICE*10 AS MULTI_PRICE
        FROM '{}'
        WHERE L_QUANTITY > 10
        ORDER BY L_QUANTITY, MULTI_PRICE
        LIMIT 10;
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
           LIMIT 10;
        """,
        [DuckDBProducer, DataFusionProducer],
    ),
    "distinct_in_project": (
        """
        SELECT DISTINCT L_TAX
        FROM '{}'
        ORDER BY L_TAX
        LIMIT 10;
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
