from __future__ import annotations

import duckdb
import pyarrow as pa

from .consumer import Consumer
from substrait_consumer.producers.producer import load_named_tables


class DuckDBConsumer(Consumer):
    """
    Adapts the DuckDB Substrait consumer to the test framework.
    """

    @classmethod
    def name(self):
        return "duckdb"

    def __init__(self, db_connection=None):
        if db_connection is not None:
            self.db_connection = db_connection
        else:
            self.db_connection = duckdb.connect()

        self.db_connection.execute("INSTALL substrait")
        self.db_connection.execute("LOAD substrait")

    def _setup(
        self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]
    ):
        self.db_connection = db_connection
        load_named_tables(db_connection, named_tables)

    def run_substrait_query(self, substrait_query: str) -> pa.Table:
        """
        Run the substrait plan against DuckDB.

        Parameters:
            substrait_query:
                A substrait plan as a json formatted string.

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """
        return self.db_connection.from_substrait_json(substrait_query).arrow()
