SQL_SCALAR = {
    "not_equal":
        """
        SELECT N_NAME
        FROM '{}'
        WHERE NOT N_NAME = 'CANADA'
        """,
    "equal":
        """
        SELECT PS_AVAILQTY, PS_PARTKEY
        FROM '{}'
        WHERE PS_AVAILQTY = PS_PARTKEY
        """,
    "is_not_distinct_from":
        """
        SELECT a 
        FROM 't' 
        WHERE a IS NOT DISTINCT FROM NULL
        """,
    "lt":
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        WHERE PS_AVAILQTY < 10
        """,
    "lte":
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        WHERE PS_AVAILQTY <= 10
        """,
    "gt":
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        WHERE PS_AVAILQTY > 10
        """,
    "gte":
        """
        SELECT PS_AVAILQTY
        FROM '{}'
        WHERE PS_AVAILQTY >= 10
        """,
    "is_not_null":
        """
        SELECT a 
        FROM 't' 
        WHERE a IS NOT NULL
        """,
    "is_null":
        """
        SELECT a 
        FROM 't' 
        WHERE a IS NULL
        """,
    "is_nan":
        """
        SELECT a, isnan(a) as isnan_a
        FROM 't' 
        """,
    "is_finite":
        """
        SELECT a, isfinite(a) as isfinite_a
        FROM 't' 
        """,
    "is_infinite":
        """
        SELECT a, isinf(a) as isinf_a
        FROM 't' 
        """,
    "coalesce":
        """
        SELECT coalesce(NULL,NULL,'test_string')
        """,
}
