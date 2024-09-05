import json
import substrait_validator as sv
from .producer import Producer, load_tables_from_parquet
from substrait_consumer.common import SubstraitUtils

import duckdb


class DuckDBProducer(Producer):
    """
    Adapts the DuckDB Substrait producer to the test framework.
    """

    def __init__(self, db_connection=None):
        if db_connection is not None:
            self._db_connection = db_connection
        else:
            self._db_connection = duckdb.connect()
            self._db_connection.execute("INSTALL substrait")
            self._db_connection.execute("LOAD substrait")

    def set_db_connection(self, db_connection):
        self._db_connection = db_connection

    def produce_substrait(self, sql_query: str, validate = False, ibis_expr: str = None) -> str:
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
            config.override_diagnostic_level(7, "warning", "info")  # warning
            # Warning: did not attempt to resolve YAML: configured recursion
            # limit for URI resolution has been reached
            config.override_diagnostic_level(2001, "warning", "info")
            sv.check_plan_valid(proto_bytes, config)
        duckdb_substrait_plan = self._db_connection.get_substrait_json(sql_query)
        proto_bytes = duckdb_substrait_plan.fetchone()[0]
        python_json = json.loads(proto_bytes)
        return json.dumps(python_json, indent=2)

    def format_sql(self, created_tables, sql_query, file_names):
        if len(file_names) > 0:
            if "read_parquet" in sql_query:
                parquet_file_path = SubstraitUtils.get_full_path(file_names)
                sql_query = sql_query.format(parquet_file_path[0])
            else:
                table_names = load_tables_from_parquet(
                    self._db_connection, created_tables, file_names
                )
                sql_query = sql_query.format(*table_names)
        return sql_query

    def name(self):
        return "DuckDBProducer"
