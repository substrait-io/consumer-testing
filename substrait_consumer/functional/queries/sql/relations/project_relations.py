from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

PROJECT_RELATIONS = {
    "project_single_col": (
        """
        SELECT *
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "project_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "project_all_col": (
        """
        SELECT *
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "extended_project": (
        """
        SELECT L_QUANTITY, L_EXTENDEDPRICE*10 AS MULTI_PRICE
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "subquery_in_project": (
        """
        SELECT C_CUSTKEY,
           (SELECT SUM(O_TOTALPRICE) 
               FROM {}
               WHERE C_CUSTKEY = O_CUSTKEY) 
                 AS total_price
           FROM {}
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "distinct_in_project": (
        """
        SELECT DISTINCT L_LINESTATUS
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "count_distinct_in_project": (
        """
        SELECT COUNT(DISTINCT L_EXTENDEDPRICE)
        FROM '{}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),

}
