
from .producer import Producer, load_tables_from_parquet

import duckdb
import pytest

from google.protobuf import json_format
from ibis_substrait.compiler.core import SubstraitCompiler


class IbisProducer(Producer):
    """
    Adapts the Ibis Substrait producer to the test framework.
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
        Produce the Ibis substrait plan using the given Ibis expression

        Parameters:
            ibis_expr:
                Ibis expression.
        Returns:
            Substrait query plan in json format.
        """
        if ibis_expr is None:
            pytest.skip("ibis expression currently undefined")
        compiler = SubstraitCompiler()

        tpch_proto_bytes = compiler.compile(ibis_expr)
        substrait_plan = json_format.MessageToJson(tpch_proto_bytes)
        return substrait_plan

    def format_sql(self, sql_query, file_names):
        if len(file_names) > 0:
            table_names = load_tables_from_parquet(
                self._db_connection, file_names
            )
            sql_query = sql_query.format(*table_names)
        return sql_query

    def name(self):
        return "IbisProducer"
