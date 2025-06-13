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
        schema_list = self._get_schema(self.table_names)
        substrait_plan_str = self._produce_isthmus_substrait(
            sql_query, schema_list, validate
        )

        return substrait_plan_str

    def _format_sql(self, sql_query):
        sql_query = re.sub(r"'(\{[0-9a-zA-Z_]+\})'", r"\1", sql_query)
        return sql_query.replace("'t'", "t")

    @staticmethod
    def _produce_isthmus_substrait(sql_string, schema_list, validate=False):
        """
        Produce the substrait plan using Isthmus.

        Parameters:
            sql_string:
                SQL query.
            schema_list:
                List of schemas.
            validate:
                Validate the Substrait plan.

        Returns:
            Substrait plan in json format.
        """
        from com.google.protobuf.util import JsonFormat as json_formatter

        from . import isthmus_java as java

        version = java.VersionClass.class_.getPackage().getSpecificationVersion()
        print("JPype version:", version)

        sql_to_substrait = java.SqlToSubstraitClass()
        java_sql_string = jpype.java.lang.String(sql_string)
        plan = sql_to_substrait.execute(java_sql_string, schema_list)
        json_plan = json_formatter.printer().print_(plan)
        if validate:
            config = sv.Config()
            config.override_diagnostic_level(1002, "info", "info")  # error
            config.override_diagnostic_level(2001, "info", "info")  # warning
            config.override_diagnostic_level(3005, "info", "info")  # warning
            config.override_diagnostic_level(1, "info", "info")  # warning
            sv.check_plan_valid(json_plan, config)
        return json_plan

    @staticmethod
    def _get_schema(local_files):
        """
        Create the list of schemas based on the given file names.  If there are no files
        give, a custom schema for the data is used.

        Parameters:
            local_files: List of file names.

        Returns:
            List of all schemas as a java list.
        """
        from . import isthmus_java as java

        arr = java.ArrayListClass()
        if local_files:
            text_schema_file = open(TPCH_SCHEMA_FILE)
            schema_string = text_schema_file.read().replace("\n", " ").split(";")[:-1]
            for create_table in schema_string:
                if "small" not in local_files[0]:
                    create_table = create_table.replace("_small", "")
                java_obj = jpype.JObject @ jpype.JString(create_table)
                arr.add(java_obj)
        java_obj = jpype.JObject @ jpype.JString(
            "CREATE TABLE T(a integer, b integer, c boolean, d boolean)"
        )
        arr.add(java_obj)

        return java.ListClass @ arr
