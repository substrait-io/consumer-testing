SQL_SCALAR = {
    "add":
        """
        SELECT PS_PARTKEY, PS_SUPPKEY, add(PS_PARTKEY, PS_SUPPKEY) AS ADD_KEY
        FROM '{}';
        """,
    "subtract":
        """
        SELECT PS_PARTKEY, PS_SUPPKEY, subtract(PS_PARTKEY, PS_SUPPKEY) AS SUBTRACT_KEY
        FROM '{}';
        """,
    "multiply":
        """
        SELECT PS_PARTKEY, multiply(PS_PARTKEY, 10) AS MULTIPLY_KEY
        FROM '{}';
        """,
    "divide":
        """
        SELECT PS_PARTKEY, divide(PS_PARTKEY, 10) AS DIVIDE_KEY
        FROM '{}';
        """,
    "modulus":
        """
        SELECT PS_PARTKEY, mod(PS_PARTKEY, 10) AS MODULO_KEY
        FROM '{}';
        """,
    "factorial":
        """
        SELECT PS_PARTKEY, factorial(PS_PARTKEY) AS FACTORIAL_KEY
        FROM '{}';
        """,
    "power":
        """
        SELECT PS_PARTKEY, power(PS_PARTKEY, 2) AS POWER_KEY
        FROM '{}';
        """,
    "sqrt":
        """
        SELECT PS_PARTKEY, sqrt(PS_PARTKEY) AS SQRT_KEY
        FROM '{}';
        """,
    "exp":
        """
        SELECT PS_PARTKEY, exp(PS_PARTKEY) AS EXP_KEY
        FROM '{}';
        """,
    "negate":
        """
        SELECT PS_PARTKEY, negate(PS_PARTKEY) AS NEGATE_KEY
        FROM '{}';
        """,
    "cos":
        """
        SELECT cos(PS_SUPPLYCOST) AS COS_SUPPLY
        FROM '{}';
        """,
    "acos":
        """
        SELECT acos(L_TAX) AS ACOS_TAX
        FROM '{}';
        """,
    "sin":
        """
        SELECT sin(PS_SUPPLYCOST) AS SIN_SUPPLY
        FROM '{}';
        """,
    "asin":
        """
        SELECT asin(L_TAX) AS ASIN_TAX
        FROM '{}';
        """,
    "tan":
        """
        SELECT tan(PS_SUPPLYCOST) AS TAN_SUPPLY
        FROM '{}';
        """,
    "atan":
        """
        SELECT atan(L_TAX) AS ATAN_TAX
        FROM '{}';
        """,
    "atan2":
        """
        SELECT atan2(L_TAX, L_TAX) AS ATAN2_TAX
        FROM '{}';
        """,
    "abs":
        """
        SELECT a, abs(a) AS ABS_A
        FROM t;
        """,
    "sign":
        """
        SELECT a, sign(a) AS SIGN_A
        FROM t;
        """,
}

SQL_AGGREGATE = {
    "sum":
        """
        SELECT sum(PS_SUPPLYCOST) AS SUM_SUPPLYCOST
        FROM '{}';
        """,
    "count":
        """
        SELECT count(PS_SUPPLYCOST) AS COUNT_SUPPLYCOST
        FROM '{}';
        """,
    "avg":
        """
        SELECT avg(PS_SUPPLYCOST) AS AVG_SUPPLYCOST
        FROM '{}';
        """,
    "min":
        """
        SELECT min(PS_SUPPLYCOST) AS MIN_SUPPLYCOST
        FROM '{}';
        """,
    "max":
        """
        SELECT max(PS_SUPPLYCOST) AS MAX_SUPPLYCOST
        FROM '{}';
        """,
    "median":
        """
        SELECT median(PS_SUPPLYCOST) AS MEDIAN_SUPPLYCOST
        FROM '{}';
        """,
    "mode":
        """
        SELECT mode(PS_SUPPLYCOST) AS MODE_SUPPLYCOST
        FROM '{}';
        """,
    "product":
        """
        SELECT product(PS_SUPPLYCOST) AS PRODUCT_SUPPLYCOST
        FROM '{}';
        """,
    "std_dev":
        """
        SELECT stddev(PS_SUPPLYCOST) AS STDDEV_SUPPLYCOST
        FROM '{}';
        """,
    "variance":
        """
        SELECT variance(PS_SUPPLYCOST) AS VARIANCE_SUPPLYCOST
        FROM '{}';
        """,
}
