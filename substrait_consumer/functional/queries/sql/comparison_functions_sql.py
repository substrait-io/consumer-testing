from substrait_consumer.producers import *

SQL_SCALAR = {
    "not_equal": (
        """
        SELECT N_NAME
        FROM '{}'
        WHERE NOT N_NAME = 'CANADA'
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "equal": (
        """
        SELECT PS_AVAILQTY, PS_PARTKEY
        FROM '{}'
        WHERE PS_AVAILQTY = PS_PARTKEY
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "is_not_distinct_from": (
        """
        SELECT a 
        FROM 't' 
        WHERE a IS NOT DISTINCT FROM NULL
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "lt": (
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        WHERE PS_AVAILQTY < 10
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "lte": (
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        WHERE PS_AVAILQTY <= 10
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "gt": (
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        WHERE PS_AVAILQTY > 9990
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "gte": (
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        WHERE PS_AVAILQTY >= 9990
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "is_not_null": (
        """
        SELECT a 
        FROM 't' 
        WHERE a IS NOT NULL
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "is_null": (
        """
        SELECT a 
        FROM 't' 
        WHERE a IS NULL
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "is_nan": (
        """
        SELECT a, isnan(a) as isnan_a
        FROM 't' 
        """,
        [DuckDBProducer],
    ),
    "is_finite": (
        """
        SELECT a, isfinite(a) as isfinite_a
        FROM 't' 
        """,
        [DuckDBProducer],
    ),
    "is_infinite": (
        """
        SELECT a, isinf(a) as isinf_a
        FROM 't' 
        """,
        [DuckDBProducer],
    ),
    "coalesce": (
        """
        SELECT coalesce(NULL,NULL,'test_string')
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "between": (
        """
        SELECT a FROM 't' WHERE a BETWEEN 1 AND 5
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
}
