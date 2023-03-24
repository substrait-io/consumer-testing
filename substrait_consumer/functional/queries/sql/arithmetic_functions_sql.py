from substrait_consumer.producers import *

SQL_SCALAR = {
    "add": (
        """
        SELECT PS_PARTKEY, PS_SUPPKEY, add(PS_PARTKEY, PS_SUPPKEY) AS ADD_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "subtract": (
        """
        SELECT PS_PARTKEY, PS_SUPPKEY, subtract(PS_PARTKEY, PS_SUPPKEY) AS SUBTRACT_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "multiply": (
        """
        SELECT PS_PARTKEY, multiply(PS_PARTKEY, 10) AS MULTIPLY_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "divide": (
        """
        SELECT PS_PARTKEY, divide(PS_PARTKEY, 10) AS DIVIDE_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "modulus": (
        """
        SELECT PS_PARTKEY, mod(PS_PARTKEY, 10) AS MODULUS_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "factorial": (
        """
        SELECT PS_PARTKEY, factorial(PS_PARTKEY) AS FACTORIAL_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "power": (
        """
        SELECT PS_PARTKEY, power(PS_PARTKEY, 2) AS POWER_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "sqrt": (
        """
        SELECT PS_PARTKEY, round(sqrt(PS_PARTKEY), 2) AS SQRT_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "exp": (
        """
        SELECT PS_PARTKEY, round(exp(PS_PARTKEY), 2) AS EXP_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "negate": (
        """
        SELECT PS_PARTKEY, negate(PS_PARTKEY) AS NEGATE_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "cos": (
        """
        SELECT round(cos(PS_SUPPLYCOST), 2) AS COS_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "acos": (
        """
        SELECT round(acos(L_TAX), 2) AS ACOS_TAX
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "sin": (
        """
        SELECT round(sin(PS_SUPPLYCOST), 2) AS SIN_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "asin": (
        """
        SELECT round(asin(L_TAX), 2) AS ASIN_TAX
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "tan": (
        """
        SELECT round(tan(PS_SUPPLYCOST), 2) AS TAN_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "atan": (
        """
        SELECT round(atan(L_TAX), 2) AS ATAN_TAX
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "atan2": (
        """
        SELECT round(atan2(L_TAX, L_TAX), 2) AS ATAN2_TAX
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "abs": (
        """
        SELECT a, abs(a) AS ABS_A
        FROM 't';
        """,
        [DuckDBProducer],
    ),
    "sign": (
        """
        SELECT a, sign(a) AS SIGN_A
        FROM 't';
        """,
        [DuckDBProducer],
    ),
}

SQL_AGGREGATE = {
    "sum": (
        """
        SELECT sum(PS_SUPPLYCOST) AS SUM_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "count": (
        """
        SELECT count(PS_SUPPLYCOST) AS COUNT_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "avg": (
        """
        SELECT round(avg(PS_SUPPLYCOST), 2) AS AVG_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "min": (
        """
        SELECT min(PS_SUPPLYCOST) AS MIN_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "max": (
        """
        SELECT max(PS_SUPPLYCOST) AS MAX_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "median": (
        """
        SELECT median(PS_SUPPLYCOST) AS MEDIAN_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
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
