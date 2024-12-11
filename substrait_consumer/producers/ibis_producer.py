
from .producer import Producer, load_named_tables

import duckdb

from google.protobuf import json_format
from ibis_substrait.compiler.core import SubstraitCompiler
from ibis import Table


class IbisProducer(Producer):
    """
    Adapts the Ibis Substrait producer to the test framework.
    """

    @classmethod
    def name(self):
        return "ibis"

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

    def _produce_substrait(self, ibis_expr: Table) -> str:
        """
        Produce the Ibis substrait plan using the given Ibis expression

        Parameters:
            ibis_expr:
                Ibis expression.
        Returns:
            Substrait query plan in json format.
        """
        compiler = SubstraitCompiler()

        tpch_proto_bytes = compiler.compile(ibis_expr)
        substrait_plan = json_format.MessageToJson(tpch_proto_bytes)
        return substrait_plan
