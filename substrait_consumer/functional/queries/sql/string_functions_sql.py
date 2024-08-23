from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer
SQL_SCALAR = {
    "concat": (
        """
        SELECT N_NAME, concat(N_NAME, N_COMMENT) AS concat_nation
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "concat_ws": (
        """
        SELECT concat_ws('.', N_NAME, N_COMMENT)
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "like": (
        """
        SELECT N_NAME
        FROM '{}'
        WHERE N_NAME LIKE 'ALGERIA';
        """,
        [DataFusionProducer, DuckDBProducer, IsthmusProducer],
    ),
    "starts_with_duckdb": (
        """
        SELECT N_NAME
        FROM '{}'
        WHERE prefix(N_NAME, 'A');
        """,
        [DuckDBProducer],
    ),
    "starts_with": (
        """
        SELECT N_NAME
        FROM '{}'
        WHERE starts_with(N_NAME, 'A');
        """,
        [DataFusionProducer],
    ),
    "ends_with": (
        """
        SELECT N_NAME
        FROM '{}'
        WHERE suffix(N_NAME, 'A');
        """,
        [DuckDBProducer],
    ),
    "substring": (
        """
        SELECT N_NAME, substr(N_NAME, 1, 3) AS substr_name
        FROM '{}';
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "substring_isthmus": (
        """
        SELECT N_NAME, SUBSTRING(N_NAME FROM 1 FOR 3) AS substr_name
        FROM '{}';
        """,
        [IsthmusProducer],
    ),
    "contains": (
        """
        SELECT N_NAME
        FROM '{}'
        WHERE contains(N_NAME, 'IA');
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "strpos": (
        """
        SELECT N_NAME, strpos(N_NAME, 'A') AS strpos_name
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "replace": (
        """
        SELECT N_NAME, replace(N_NAME, 'A', 'a') AS replace_name
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "repeat": (
        """
        SELECT N_NAME, repeat(N_NAME, 2) AS repeated_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "reverse": (
        """
        SELECT N_NAME, reverse(N_NAME) AS reversed_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "lower": (
        """
        SELECT N_NAME, lower(N_NAME) AS lowercase_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "upper": (
        """
        SELECT O_COMMENT, upper(O_COMMENT) AS uppercase_O_COMMENT
        FROM '{}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "char_length": (
        """
        SELECT N_NAME, length(N_NAME) AS char_length_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "bit_length": (
        """
        SELECT N_NAME, bit_length(N_NAME) AS bit_length_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "ltrim": (
        """
        SELECT N_NAME, ltrim(N_NAME, 'A') AS ltrim_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "rtrim": (
        """
        SELECT N_NAME, rtrim(N_NAME, 'A') AS rtrim_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "trim": (
        """
        SELECT N_NAME, trim(N_NAME, 'A') AS trim_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "lpad": (
        """
        SELECT N_NAME, lpad(N_NAME, 10, ' ') AS lpad_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "rpad": (
        """
        SELECT N_NAME, rpad(N_NAME, 10, ' ') AS rpad_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "left": (
        """
        SELECT N_NAME, left(N_NAME, 2) AS left_extract_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "right": (
        """
        SELECT N_NAME, right(N_NAME, 2) AS right_extract_N_NAME
        FROM '{}'
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
}

SQL_AGGREGATE = {
    "string_agg": (
        """
        SELECT N_NAME, string_agg(N_NAME, ',')
        FROM '{}'
        GROUP BY N_NAME
        ORDER BY N_NAME
        """,
        [DuckDBProducer],
    ),
}
