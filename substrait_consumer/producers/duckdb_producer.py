import json
from typing import Optional

import duckdb
import pyarrow as pa
import substrait_validator as sv

from .producer import Producer, load_named_tables

class DuckDBProducer(Producer):
    """
    Adapts the DuckDB Substrait producer to the test framework.
    """

    @classmethod
    def name(self):
        return "duckdb"

    def __init__(self, db_connection=None):
        if db_connection is not None:
            self._db_connection = db_connection
        else:
            self._db_connection = duckdb.connect()
            self._db_connection.execute("INSTALL substrait")
            self._db_connection.execute("LOAD substrait")

    def _setup(
        self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]
    ):
        self._db_connection = db_connection
        load_named_tables(self._db_connection, named_tables)

    def _produce_substrait(
        self, sql_query: str, validate=False, ibis_expr: str = None
    ) -> str:
        """
        Produce the DuckDB substrait plan using the given SQL query.

        Parameters:
            sql_query:
                SQL query.
        Returns:
            Substrait query plan in json format.
        """
        if validate:
            proto_bytes = self._db_connection.get_substrait(sql_query).fetchone()[0]
            config = sv.Config()
            # Warning: cannot automatically determine whether plan version
            # is compatible with the Substrait version
            config.override_diagnostic_level(7, "info", "info")  # warning
            # Warning: did not attempt to resolve YAML: configured recursion
            # limit for URI resolution has been reached
            config.override_diagnostic_level(2001, "info", "info")
            sv.check_plan_valid(proto_bytes, config)
        duckdb_substrait_plan = self._db_connection.get_substrait_json(sql_query)
        proto_bytes = duckdb_substrait_plan.fetchone()[0]
        python_json = json.loads(proto_bytes)
        return json.dumps(python_json, indent=2)

    def run_sql_query(self, sql_query: str) -> Optional[pa.Table]:
        sql_query = self.format_sql(sql_query)
        result = self._db_connection.query(f"{sql_query}")
        if result is not None:
            return result.arrow()
