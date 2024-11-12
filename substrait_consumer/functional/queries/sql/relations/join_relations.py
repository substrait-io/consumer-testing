from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

JOIN_RELATIONS = {
    "inner_join": (
        """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME,
            o.O_ORDERKEY
        FROM
            '{customer}' c
        INNER JOIN
            '{orders}' o
        ON
            c.C_CUSTKEY = o.O_CUSTKEY;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "left_join": (
        """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME,
            o.O_ORDERKEY
        FROM
            '{customer}' c
        LEFT JOIN
            '{orders}' o
        ON
            c.C_CUSTKEY = o.O_CUSTKEY;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "right_join": (
        """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME,
            o.O_ORDERKEY
        FROM
            '{customer}' c
        RIGHT JOIN
            '{orders}' o
        ON
            c.C_CUSTKEY = o.O_CUSTKEY;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "full_join": (
        """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME,
            o.O_ORDERKEY
        FROM
            '{customer}' c
        FULL JOIN
            '{orders}' o
        ON
            c.C_CUSTKEY = o.O_CUSTKEY;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "cross_join": (
        """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME,
            o.O_ORDERKEY
        FROM
            '{customer}' c
        CROSS JOIN
            '{orders}' o
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "left_semi_join": (
        """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME
        FROM
            '{customer}' c
        WHERE
            EXISTS (
                SELECT 1
                FROM '{orders}' o
                WHERE o.O_CUSTKEY = c.C_CUSTKEY
            );
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "right_semi_join": (
        """
        SELECT
            o.O_ORDERKEY,
            o.O_CUSTKEY
        FROM
            '{orders}' o
        WHERE
            EXISTS (
                SELECT 1
                FROM '{customer}' c
                WHERE c.C_CUSTKEY = o.O_CUSTKEY
            );
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "left_anti_join": (
        """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME
        FROM
            '{customer}' c
        WHERE
            NOT EXISTS (
                SELECT 1
                FROM '{orders}' o
                WHERE o.O_CUSTKEY = c.C_CUSTKEY
            );
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "right_anti_join": (
        """
        SELECT
            o.O_ORDERKEY,
            o.O_CUSTKEY
        FROM
            '{orders}' o
        WHERE
            NOT EXISTS (
                SELECT 1
                FROM '{lineitem}' l
                WHERE l.L_ORDERKEY = o.O_ORDERKEY
            );
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "left_single_join": (
        """
        SELECT
            c1.C_CUSTKEY AS c1key,
            c1.C_NAME AS c1name,
            c1.C_NATIONKEY AS c1nationakey,
            c2.C_CUSTKEY AS c2key,
            c2.C_NAME AS c2name,
            c2.C_NATIONKEY AS c2nationakey
        FROM
            '{customer}' c1
        LEFT JOIN
            '{customer}' c2
        ON
            c1.C_NATIONKEY = c2.C_NATIONKEY
            AND c1.C_CUSTKEY <> c2.C_CUSTKEY;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "right_single_join": (
        """
        SELECT
            c1.C_CUSTKEY AS c1key,
            c1.C_NAME AS c1name,
            c1.C_NATIONKEY AS c1nationakey,
            c2.C_CUSTKEY AS c2key,
            c2.C_NAME AS c2name,
            c2.C_NATIONKEY AS c2nationakey
        FROM
            '{customer}' c1
        RIGHT JOIN
            '{customer}' c2
        ON
            c1.C_NATIONKEY = c2.C_NATIONKEY
            AND c1.C_CUSTKEY <> c2.C_CUSTKEY;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "left_mark_join": (
        """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME,
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM '{orders}' o
                    WHERE o.O_CUSTKEY = c.C_CUSTKEY
                ) THEN 'Marked'
                ELSE 'Not Marked'
            END AS mark_status
        FROM
            '{customer}' c;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "right_mark_join": (
        """
        SELECT
            o.O_ORDERKEY,
            o.O_CUSTKEY,
            CASE 
                WHEN EXISTS (
                    SELECT 1
                    FROM '{customer}' c
                    WHERE c.C_CUSTKEY = o.O_CUSTKEY
                ) THEN 'Marked'
                ELSE 'Not Marked'
            END AS mark_status
        FROM
            '{orders}' o;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
