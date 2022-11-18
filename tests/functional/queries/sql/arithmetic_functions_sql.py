from tests.producers import *

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
        SELECT PS_PARTKEY, mod(PS_PARTKEY, 10) AS MODULO_KEY
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
        SELECT PS_PARTKEY, sqrt(PS_PARTKEY) AS SQRT_KEY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "exp": (
        """
        SELECT PS_PARTKEY, exp(PS_PARTKEY) AS EXP_KEY
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
        SELECT cos(PS_SUPPLYCOST) AS COS_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "acos": (
        """
        SELECT acos(L_TAX) AS ACOS_TAX
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "sin": (
        """
        SELECT sin(PS_SUPPLYCOST) AS SIN_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "asin": (
        """
        SELECT asin(L_TAX) AS ASIN_TAX
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "tan": (
        """
        SELECT tan(PS_SUPPLYCOST) AS TAN_SUPPLY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "atan": (
        """
        SELECT atan(L_TAX) AS ATAN_TAX
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "atan2": (
        """
        SELECT atan2(L_TAX, L_TAX) AS ATAN2_TAX
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
        SELECT avg(PS_SUPPLYCOST) AS AVG_SUPPLYCOST
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
        SELECT stddev(PS_SUPPLYCOST) AS STDDEV_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "variance": (
        """
        SELECT variance(PS_SUPPLYCOST) AS VARIANCE_SUPPLYCOST
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
}
