from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

DDL_RELATIONS = {
    "create_table": (
        """
        CREATE TABLE customer2 (
            custkey INT NOT NULL,
            name VARCHAR NOT NULL,
            address VARCHAR NOT NULL,
        )
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "drop_table": (
        """
        DROP TABLE '{}';
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "alter_table": (
        """
        ALTER TABLE '{}'
        ADD email VARCHAR;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "alter_column": (
        """
        ALTER TABLE '{}'
        RENAME COLUMN c_address TO c_street_address;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "drop_column": (
        """
        ALTER TABLE '{}'
        DROP COLUMN c_address;
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "create_view": (
        """
        CREATE VIEW customer_view AS
        SELECT 
            C_CUSTKEY,
            C_NAME,
        FROM 
            '{}';
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
    "create_or_replace_view": (
        """
        CREATE OR REPLACE VIEW customer_view AS
        SELECT 
            C_CUSTKEY,
            C_NAME,
        FROM 
            '{}';
        """,
        [DuckDBProducer, DataFusionProducer, IsthmusProducer],
    ),
}
