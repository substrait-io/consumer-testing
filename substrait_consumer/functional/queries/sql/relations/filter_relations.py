from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

FILTER_RELATIONS = {
    "where_equal_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{}'
        WHERE L_DISCOUNT = L_TAX
        ORDER BY L_DISCOUNT
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_not_equal_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{}'
        WHERE L_DISCOUNT != L_TAX
        ORDER BY L_DISCOUNT, L_TAX
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_gt_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{}'
        WHERE L_DISCOUNT > L_TAX
        ORDER BY L_DISCOUNT, L_TAX
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_gte_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{}'
        WHERE L_DISCOUNT >= L_TAX
        ORDER BY L_DISCOUNT, L_TAX
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_lt_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{}'
        WHERE L_DISCOUNT < L_TAX
        ORDER BY L_DISCOUNT, L_TAX
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_lte_multi_col": (
        """
        SELECT L_DISCOUNT, L_TAX
        FROM '{}'
        WHERE L_DISCOUNT <= L_TAX
        ORDER BY L_DISCOUNT, L_TAX
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_like": (
        """
        SELECT L_SHIPINSTRUCT, L_ORDERKEY
        FROM '{}'
        WHERE L_SHIPINSTRUCT LIKE '%DELIVER IN PERSON%'
        ORDER BY L_ORDERKEY
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_between": (
        """
        SELECT L_ORDERKEY
        FROM '{}'
        WHERE L_ORDERKEY BETWEEN 20 AND 50
        LIMIT 20;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_in": (
        """
        SELECT L_ORDERKEY
        FROM '{}'
        WHERE L_ORDERKEY IN (1, 2, 3)
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_or": (
        """
        SELECT L_ORDERKEY, L_SHIPINSTRUCT
        FROM '{}'
        WHERE L_ORDERKEY = 2 OR L_ORDERKEY = 3
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "where_and": (
        """
        SELECT L_ORDERKEY, L_SHIPINSTRUCT
        FROM '{}'
        WHERE L_ORDERKEY = 2 AND L_SHIPINSTRUCT = 'TAKE BACK RETURN'
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "having": (
        """
        SELECT L_QUANTITY, COUNT(*)
        FROM '{}'
        GROUP BY L_QUANTITY
        HAVING COUNT(*) > 12100
        ORDER BY L_QUANTITY
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
