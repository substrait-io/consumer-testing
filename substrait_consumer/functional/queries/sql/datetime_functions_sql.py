from substrait_consumer.producers import *

SQL_SCALAR = {
    "extract": (
        """
        SELECT L_SHIPDATE, extract(year FROM L_SHIPDATE)
        FROM '{}';
        """,
        [DuckDBProducer, IsthmusProducer],
    ),
    "add": (
        """
        SELECT L_SHIPDATE, L_SHIPDATE + INTERVAL 5 DAY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "add_intervals": (
        """
        SELECT INTERVAL 1 HOUR + INTERVAL 5 HOUR
        """,
        [DuckDBProducer],
    ),
    "subtract": (
        """
        SELECT L_SHIPDATE, L_SHIPDATE - INTERVAL 5 DAY
        FROM '{}';
        """,
        [DuckDBProducer],
    ),
    "lt": (
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE < L_RECEIPTDATE
        FROM '{}';
        """,
        [DuckDBProducer, IsthmusProducer],
    ),
    "lte": (
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE <= L_RECEIPTDATE
        FROM '{}';
        """,
        [DuckDBProducer, IsthmusProducer],
    ),
    "gt": (
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE > L_RECEIPTDATE
        FROM '{}';
        """,
        [DuckDBProducer, IsthmusProducer],
    ),
    "gte": (
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE >= L_RECEIPTDATE
        FROM '{}';
        """,
        [DuckDBProducer, IsthmusProducer],
    ),
}
