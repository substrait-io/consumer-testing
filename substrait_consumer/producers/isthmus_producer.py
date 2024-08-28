import duckdb
from .producer import Producer, load_tables_from_parquet
from ibis_substrait.compiler.core import SubstraitCompiler

from substrait_consumer.context import get_schema, produce_isthmus_substrait


class IsthmusProducer(Producer):
    """
    Adapts the Isthmus Substrait producer to the test framework.
    """
    def __init__(self, db_connection=None):
        if db_connection is not None:
            self._db_connection = db_connection
        else:
            self._db_connection = duckdb.connect()

        self._db_connection.execute("INSTALL substrait")
        self._db_connection.execute("LOAD substrait")
        self.compiler = SubstraitCompiler()
        self.file_names = None

    def set_db_connection(self, db_connection):
        self._db_connection = db_connection

    def produce_substrait(self, sql_query: str, validate=False, ibis_expr: str=None) -> str:
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
        schema_list = get_schema(self.file_names)
        substrait_plan_str = produce_isthmus_substrait(sql_query, schema_list, validate)

        return substrait_plan_str

    def format_sql(self, created_tables, sql_query, file_names):
        sql_query = sql_query.replace("'{}'", "{}")
        sql_query = sql_query.replace("'t'", "t")
        if len(file_names) > 0:
            self.file_names = file_names
            table_names = load_tables_from_parquet(
                self._db_connection, created_tables, file_names
            )
            sql_query = sql_query.format(*table_names)
        return sql_query

    def name(self):
        return "IsthmusProducer"
