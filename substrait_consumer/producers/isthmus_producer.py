import re
from pathlib import Path

import duckdb
from ibis_substrait.compiler.core import SubstraitCompiler
import jpype.imports
import substrait_validator as sv

from .producer import SQLProducer, load_named_tables

DATA_DIR = Path(__file__).parent.parent / "data"
TPCH_SCHEMA_FILE = DATA_DIR / "tpch_parquet" / "schema.sql"


class IsthmusProducer(SQLProducer):
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

        text_schema_file = open(TPCH_SCHEMA_FILE)
        schema_string = text_schema_file.read() + "\nCREATE TABLE T(a integer, b integer, c boolean, d boolean)"

        from . import isthmus_java as java
        self.catalog = java.SubstraitCreateStatementParser.processCreateStatementsToCatalog(schema_string)


    def _produce_substrait(self, sql_query: str, validate=False) -> str:
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
        from . import isthmus_java as java
        from com.google.protobuf.util import JsonFormat as json_formatter

        sql_to_substrait = java.SqlToSubstraitClass()
        java_sql_string = jpype.java.lang.String(sql_query)

        plan = sql_to_substrait.execute(java_sql_string, self.catalog)
        json_plan = json_formatter.printer().print_(plan)
        if validate:
            config = sv.Config()
            config.override_diagnostic_level(1002, "info", "info")  # error
            config.override_diagnostic_level(2001, "info", "info")  # warning
            config.override_diagnostic_level(3005, "info", "info")  # warning
            config.override_diagnostic_level(1, "info", "info")  # warning
            sv.check_plan_valid(json_plan, config)
        return json_plan

    def _format_sql(self, sql_query):
        sql_query = re.sub(r"'(\{[0-9a-zA-Z_]+\})'", r"\1", sql_query)
        return sql_query.replace("'t'", "t")
