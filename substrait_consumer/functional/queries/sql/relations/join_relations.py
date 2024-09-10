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
            '{}' c
        INNER JOIN
            '{}' o
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
            '{}' c
        LEFT JOIN
            '{}' o
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
            '{}' c
        RIGHT JOIN
            '{}' o
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
            '{}' c
        FULL JOIN
            '{}' o
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
            '{}' c
        CROSS JOIN
            '{}' o
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "left_semi_join": (
        """
        SELECT
            c.C_CUSTKEY,
            c.C_NAME
        FROM
            '{}' c
        WHERE
            EXISTS (
                SELECT 1
                FROM '{}' o
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
            '{}' o
        WHERE
            EXISTS (
                SELECT 1
                FROM '{}' c
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
            '{}' c
        WHERE
            NOT EXISTS (
                SELECT 1
                FROM '{}' o
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
            '{}' o
        WHERE
            NOT EXISTS (
                SELECT 1
                FROM '{}' l
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
            '{}' c1
        LEFT JOIN
            '{}' c2
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
            '{}' c1
        RIGHT JOIN
            '{}' c2
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
                    FROM '{}' o
                    WHERE o.O_CUSTKEY = c.C_CUSTKEY
                ) THEN 'Marked'
                ELSE 'Not Marked'
            END AS mark_status
        FROM
            '{}' c;
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
                    FROM '{}' c
                    WHERE c.C_CUSTKEY = o.O_CUSTKEY
                ) THEN 'Marked'
                ELSE 'Not Marked'
            END AS mark_status
        FROM
            '{}' o;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
