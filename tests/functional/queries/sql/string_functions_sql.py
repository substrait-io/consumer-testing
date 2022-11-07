SQL_SCALAR = {
    "concat": """
        SELECT N_NAME, concat(N_NAME, N_COMMENT) AS concat_nation
        FROM '{}';
        """,
    "concat_ws": """
        SELECT concat_ws('.', N_NAME, N_COMMENT)
        FROM '{}';
        """,
    "like": """
        SELECT N_NAME
        FROM '{}'
        WHERE N_NAME LIKE 'ALGERIA';
        """,
    "starts_with": """
        SELECT N_NAME
        FROM '{}'
        WHERE prefix(N_NAME, 'A');
        """,
    "ends_with": """
        SELECT N_NAME
        FROM '{}'
        WHERE suffix(N_NAME, 'A');
        """,
    "substring": """
        SELECT N_NAME, substr(N_NAME, 1, 3) AS substr_name
        FROM '{}';
        """,
    "contains": """
        SELECT N_NAME
        FROM '{}'
        WHERE contains(N_NAME, 'IA');
        """,
    "strpos": """
        SELECT N_NAME, strpos(N_NAME, 'A') AS strpos_name
        FROM '{}'
        """,
    "replace": """
        SELECT N_NAME, replace(N_NAME, 'A', 'a') AS replace_name
        FROM '{}'
        """,
    "repeat": """
        SELECT N_NAME, repeat(N_NAME, 2) AS repeated_N_NAME
        FROM '{}'
        """,
    "reverse": """
        SELECT N_NAME, reverse(N_NAME) AS reversed_N_NAME
        FROM '{}'
        """,
    "lower": """
        SELECT N_NAME, lower(N_NAME) AS lowercase_N_NAME
        FROM '{}'
        """,
    "upper": """
        SELECT O_COMMENT, upper(O_COMMENT) AS uppercase_O_COMMENT
        FROM '{}'
        """,
    "char_length": """
        SELECT N_NAME, length(N_NAME) AS char_length_N_NAME
        FROM '{}'
        """,
    "bit_length": """
        SELECT N_NAME, bit_length(N_NAME) AS bit_length_N_NAME
        FROM '{}'
        """,
    "ltrim": """
        SELECT N_NAME, ltrim(N_NAME, 'A') AS ltrim_N_NAME
        FROM '{}'
        """,
    "rtrim": """
        SELECT N_NAME, rtrim(N_NAME, 'A') AS rtrim_N_NAME
        FROM '{}'
        """,
    "trim": """
        SELECT N_NAME, trim(N_NAME, 'A') AS trim_N_NAME
        FROM '{}'
        """,
    "lpad": """
        SELECT N_NAME, lpad(N_NAME, 10, ' ') AS lpad_N_NAME
        FROM '{}'
        """,
    "rpad": """
        SELECT N_NAME, rpad(N_NAME, 10, ' ') AS rpad_N_NAME
        FROM '{}'
        """,
    "left": """
        SELECT N_NAME, left(N_NAME, 2) AS left_extract_N_NAME
        FROM '{}'
        """,
    "right": """
        SELECT N_NAME, right(N_NAME, 2) AS right_extract_N_NAME
        FROM '{}'
        """,
}

SQL_AGGREGATE = {
    "string_agg": """
        SELECT N_NAME, string_agg(N_NAME, ',')
        FROM '{}'
        GROUP BY N_NAME
        """,
}
