SQL_SCALAR = {
    "add":
        """
        SELECT L_TAX, L_DISCOUNT, add(L_TAX, L_DISCOUNT) AS ADD_KEY
        FROM '{}';
        """,
    "subtract":
        """
        SELECT L_TAX, L_DISCOUNT, subtract(L_TAX, L_DISCOUNT) AS SUBTRACT_KEY
        FROM '{}';
        """,
    "multiply":
        """
        SELECT L_TAX, L_EXTENDEDPRICE, multiply(L_TAX, L_EXTENDEDPRICE) AS MULTIPLY_KEY
        FROM '{}';
        """,
    "divide":
        """
        SELECT L_TAX, L_EXTENDEDPRICE, divide(L_TAX, L_EXTENDEDPRICE) AS DIVIDE_KEY
        FROM '{}';
        """,
    "modulus":
        """
        SELECT L_EXTENDEDPRICE, L_TAX, mod(L_EXTENDEDPRICE, L_TAX) AS MODULUS_KEY
        FROM '{}';
        """,
}

SQL_AGGREGATE = {
    "sum":
        """
        SELECT sum(L_EXTENDEDPRICE) AS SUM_EXTENDEDPRICE
        FROM '{}';
        """,
    "avg":
        """
        SELECT avg(L_EXTENDEDPRICE) AS AVG_EXTENDEDPRICE
        FROM '{}';
        """,
    "min":
        """
        SELECT min(L_EXTENDEDPRICE) AS MIN_EXTENDEDPRICE
        FROM '{}';
        """,
    "max":
        """
        SELECT max(L_EXTENDEDPRICE) AS MAX_EXTENDEDPRICE
        FROM '{}';
        """,
}
