import re

import duckdb
from .producer import Producer, load_named_tables
from ibis_substrait.compiler.core import SubstraitCompiler

from substrait_consumer.context import get_schema, produce_isthmus_substrait


class IsthmusProducer(Producer):
    """
    Adapts the Isthmus Substrait producer to the test framework.
    """

    @classmethod
    def name(self):
        return "isthmus"

    def __init__(self, db_connection=None):
        if db_connection is not None:
            self._db_connection = db_connection
        else:
            self._db_connection = duckdb.connect()

        self._db_connection.execute("INSTALL substrait")
        self._db_connection.execute("LOAD substrait")
        self.compiler = SubstraitCompiler()
        self.table_names = None

    def _setup(
        self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]
    ):
        self._db_connection = db_connection
        self.table_names = list(named_tables.keys())
        load_named_tables(self._db_connection, named_tables)

    def _produce_substrait(
        self, sql_query: str, validate=False, ibis_expr: str = None
    ) -> str:
        """
        Produce the Isthmus substrait plan using the given SQL query.

        Parameters:
            sql_query:
                SQL query.
            validate:
                Validate the Substrait plan.
        Returns:
            Substrait query plan in json format.
        """
        schema_list = get_schema(self.table_names)
        substrait_plan_str = produce_isthmus_substrait(sql_query, schema_list, validate)

        return substrait_plan_str

    def _format_sql(self, sql_query):
        sql_query = re.sub(r"'(\{[0-9a-zA-Z_]+\})'", r"\1", sql_query)
        return sql_query.replace("'t'", "t")
