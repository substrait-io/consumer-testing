import json
from .producer import Producer, load_tables_from_parquet

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

    def produce_substrait(self, sql_query: str, ibis_expr: str = None) -> str:
        """
        Produce the DuckDB substrait plan using the given SQL query.

        Parameters:
            sql_query:
                SQL query.
        Returns:
            Substrait query plan in json format.
        """
        duckdb_substrait_plan = self._db_connection.get_substrait_json(sql_query)
        proto_bytes = duckdb_substrait_plan.fetchone()[0]
        python_json = json.loads(proto_bytes)
        return json.dumps(python_json, indent=2)

    def format_sql(self, created_tables, sql_query, file_names):
        if len(file_names) > 0:
            table_names = load_tables_from_parquet(
                self._db_connection, created_tables, file_names
            )
            sql_query = sql_query.format(*table_names)
        return sql_query

    def name(self):
        return "DuckDBProducer"
