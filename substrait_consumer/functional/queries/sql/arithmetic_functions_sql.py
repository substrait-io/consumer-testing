from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

SQL_SCALAR = {
    "add": (
        """
        SELECT PS_PARTKEY, PS_SUPPKEY, PS_PARTKEY + PS_SUPPKEY AS ADD_KEY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "subtract": (
        """
        SELECT PS_PARTKEY, PS_SUPPKEY, PS_PARTKEY - PS_SUPPKEY AS SUBTRACT_KEY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "multiply": (
        """
        SELECT PS_PARTKEY, PS_PARTKEY * 10 AS MULTIPLY_KEY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "divide": (
        """
        SELECT PS_PARTKEY, PS_PARTKEY / 10 AS DIVIDE_KEY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "modulus": (
        """
        SELECT PS_PARTKEY, mod(PS_PARTKEY, 10) AS MODULUS_KEY
        FROM '{}'
        LIMIT 10;
        """,
        [DuckDBProducer, IsthmusProducer],
    ),
    "factorial": (
        """
        SELECT N_NATIONKEY, factorial(N_NATIONKEY) AS FACTORIAL_KEY
        FROM '{}'
        WHERE N_NATIONKEY <= 10
        LIMIT 100;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "power": (
        """
        SELECT PS_PARTKEY, power(PS_PARTKEY, 2) AS POWER_KEY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "sqrt": (
        """
        SELECT PS_PARTKEY, round(sqrt(CAST(PS_PARTKEY AS DOUBLE)), 2) AS SQRT_KEY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "exp": (
        """
        SELECT PS_PARTKEY, round(exp(CAST(PS_PARTKEY AS DOUBLE)), 2) AS EXP_KEY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "negate": (
        """
        SELECT PS_PARTKEY, negate(PS_PARTKEY) AS NEGATE_KEY
        FROM '{}'
        LIMIT 10;
        """,
        [DuckDBProducer],
    ),
    "cos": (
        """
        SELECT round(cos(CAST(ps_supplycost AS DOUBLE)), 2) AS COS_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "acos": (
        """
        SELECT round(acos(CAST(l_tax AS DOUBLE)), 2) AS ACOS_TAX
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "sin": (
        """
        SELECT round(sin(CAST(ps_supplycost AS DOUBLE)), 2) AS SIN_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "asin": (
        """
        SELECT round(asin(CAST(l_tax AS DOUBLE)), 2) AS ASIN_TAX
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "tan": (
        """
        SELECT round(tan(CAST(ps_supplycost AS DOUBLE)), 2) AS TAN_SUPPLY
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "atan": (
        """
        SELECT round(atan(CAST(l_tax AS DOUBLE)), 2) AS ATAN_TAX
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "atan2": (
        """
        SELECT round(atan2(CAST(l_tax AS DOUBLE), CAST(l_tax AS DOUBLE)), 2) AS ATAN2_TAX
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "abs": (
        """
        SELECT a, abs(a) AS ABS_A
        FROM 't'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "sign": (
        """
        SELECT a, sign(a) AS SIGN_A
        FROM 't'
        LIMIT 10;
        """,
        [DuckDBProducer, IsthmusProducer],
    ),
}

SQL_AGGREGATE = {
    "sum": (
        """
        SELECT sum(PS_SUPPLYCOST) AS SUM_SUPPLYCOST
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "count": (
        """
        SELECT count(PS_SUPPLYCOST) AS COUNT_SUPPLYCOST
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "count_star": (
        """
        SELECT count(*)
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "avg": (
        """
        SELECT round(avg(PS_SUPPLYCOST), 2) AS AVG_SUPPLYCOST
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "min": (
        """
        SELECT min(PS_SUPPLYCOST) AS MIN_SUPPLYCOST
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "max": (
        """
        SELECT max(PS_SUPPLYCOST) AS MAX_SUPPLYCOST
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "median": (
        """
        SELECT median(PS_SUPPLYCOST) AS MEDIAN_SUPPLYCOST
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "mode": (
        """
        SELECT mode(PS_SUPPLYCOST) AS MODE_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "product": (
        """
        SELECT product(PS_SUPPLYCOST) AS PRODUCT_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "std_dev": (
        """
        SELECT round(stddev(PS_SUPPLYCOST), 2) AS STDDEV_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "variance": (
        """
        SELECT round(variance(PS_SUPPLYCOST), 2) AS VARIANCE_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
}
