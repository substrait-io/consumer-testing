SQL_SCALAR = {
    "extract":
        """
        SELECT L_SHIPDATE, extract('year' FROM L_SHIPDATE)
        FROM '{}';
        """,
    "add":
        """
        SELECT L_SHIPDATE, L_SHIPDATE + INTERVAL 5 DAY
        FROM '{}';
        """,
    "add_intervals":
        """
        SELECT INTERVAL 1 HOUR + INTERVAL 5 HOUR
        """,
    "subtract":
        """
        SELECT L_SHIPDATE, L_SHIPDATE - INTERVAL 5 DAY
        FROM '{}';
        """,
    "lt":
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE < L_RECEIPTDATE
        FROM '{}';
        """,
    "lte":
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE <= L_RECEIPTDATE
        FROM '{}';
        """,
    "gt":
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE > L_RECEIPTDATE
        FROM '{}';
        """,
    "gte":
        """
        SELECT L_COMMITDATE, L_RECEIPTDATE, L_COMMITDATE >= L_RECEIPTDATE
        FROM '{}';
        """,
}
