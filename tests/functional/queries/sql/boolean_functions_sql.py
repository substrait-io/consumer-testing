SQL_SCALAR = {
    "or":
        """
        SELECT a
        FROM 't'
        WHERE a = 5 OR a = 7;
        """,
    "and":
        """
        SELECT a, b
        FROM 't'
        WHERE a < 5 AND b = 1;
        """,
    "not":
        """
        SELECT c FROM 't' WHERE NOT c
        """,
    "xor":
        """
        SELECT a, b,  xor(a, b) AS xor_a_b
        FROM 't';
        """,
}

SQL_AGGREGATE = {
    "bool_and":
        """
        SELECT bool_and(c) AS bool_and_c
        FROM 't'
        """,
    "bool_or":
        """
        SELECT bool_or(c) AS bool_or_c
        FROM 't'
        """,
}
