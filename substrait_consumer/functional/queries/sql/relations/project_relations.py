from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

PROJECT_RELATIONS = {
    "project_single_col": (
        """
        SELECT *
        FROM '{lineitem}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "project_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{lineitem}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "project_all_col": (
        """
        SELECT *
        FROM '{region}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "extended_project": (
        """
        SELECT L_QUANTITY, L_EXTENDEDPRICE*10 AS MULTI_PRICE
        FROM '{lineitem}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "subquery_in_project": (
        """
        SELECT C_CUSTKEY,
           (SELECT SUM(O_TOTALPRICE) 
               FROM '{orders}'
               WHERE C_CUSTKEY = O_CUSTKEY) 
                 AS total_price
           FROM '{customer}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "distinct_in_project": (
        """
        SELECT DISTINCT L_LINESTATUS
        FROM '{lineitem}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "count_distinct_in_project": (
        """
        SELECT COUNT(DISTINCT L_EXTENDEDPRICE)
        FROM '{lineitem}'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),

}
