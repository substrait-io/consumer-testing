from substrait_consumer.producers import *

SQL_AGGREGATE = {
    "approx_count_distinct": (
        """
        SELECT approx_count_distinct(l_comment)
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
}
