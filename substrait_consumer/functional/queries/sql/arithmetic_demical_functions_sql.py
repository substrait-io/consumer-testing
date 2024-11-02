from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer

SQL_SCALAR = {
    "add": (
        """
        SELECT L_TAX, L_DISCOUNT, add(L_TAX, L_DISCOUNT) AS ADD_KEY
        FROM '{lineitem}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "subtract": (
        """
        SELECT L_TAX, L_DISCOUNT, subtract(L_TAX, L_DISCOUNT) AS SUBTRACT_KEY
        FROM '{lineitem}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "multiply": (
        """
        SELECT L_TAX, L_EXTENDEDPRICE, round(multiply(L_TAX, L_EXTENDEDPRICE), 2) AS MULTIPLY_KEY
        FROM '{lineitem}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "divide": (
        """
        SELECT L_TAX, L_EXTENDEDPRICE, round(divide(L_EXTENDEDPRICE, L_TAX), 2) AS DIVIDE_KEY
        FROM '{lineitem}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "modulus": (
        """
        SELECT L_EXTENDEDPRICE, L_TAX, round(mod(L_EXTENDEDPRICE, L_TAX), 2) AS MODULUS_KEY
        FROM '{lineitem}'
        LIMIT 10;
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
}

SQL_AGGREGATE = {
    "sum": (
        """
        SELECT sum(L_EXTENDEDPRICE) AS SUM_EXTENDEDPRICE
        FROM '{lineitem}';
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "avg": (
        """
        SELECT round(avg(L_EXTENDEDPRICE), 2) AS AVG_EXTENDEDPRICE
        FROM '{lineitem}';
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "min": (
        """
        SELECT min(L_EXTENDEDPRICE) AS MIN_EXTENDEDPRICE
        FROM '{lineitem}';
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
    "max": (
        """
        SELECT max(L_EXTENDEDPRICE) AS MAX_EXTENDEDPRICE
        FROM '{lineitem}';
        """,
        [DataFusionProducer, DuckDBProducer],
    ),
}
