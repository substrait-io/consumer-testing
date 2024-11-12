from pathlib import Path
import substrait_validator as sv
from .producer import Producer
import substrait.gen.proto.plan_pb2 as plan_pb2
from datafusion import SessionContext
from datafusion import substrait as ss
from google.protobuf.json_format import MessageToJson
import pyarrow as pa

from substrait_consumer.common import SubstraitUtils
from substrait_consumer.consumers.consumer import COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D


class DataFusionProducer(Producer):
    """
    Adapts the DataFusion Substrait producer to the test framework.
    """
    def __init__(self, db_connection=None):
        self._ctx = SessionContext()
        if db_connection is not None:
            self._db_connection = db_connection
        else:
            self._db_connection = db_connection

    def _setup(
        self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]
    ):
        self._db_connection = db_connection
        self.register_named_tables(named_tables)

    def _produce_substrait(
        self, sql_query: str, validate=False, ibis_expr: str = None
    ) -> str:
        """
        Produce the DataFusion substrait plan using the given SQL query.

        Parameters:
            sql_query:
                SQL query.
        Returns:
            Substrait query plan in json format.
        Raises:
            DecodeError if the plan cannot be parsed.
        """
        substrait_proto = plan_pb2.Plan()

        substrait_plan = ss.serde.serialize_to_plan(sql_query, self._ctx)
        substrait_plan_bytes = substrait_plan.encode()
        if validate:
            config = sv.Config()
            # Error: missing required protobuf field: struct
            config.override_diagnostic_level(1002, "error", "info")
            # Warning: cannot automatically determine whether plan version
            # is compatible with the Substrait version
            config.override_diagnostic_level(7, "warning", "info") # warning
            # Error: URI reference
            config.override_diagnostic_level(3001, "error", "info")
            sv.check_plan_valid(substrait_plan_bytes, config)
        substrait_proto.ParseFromString(substrait_plan_bytes)

        return MessageToJson(substrait_proto)

    def register_named_tables(self, named_tables):
        """
        Register named_tables to the datafusion session context.

        Parameters:
            named_tables:
                A `dict` mapping table names to local file paths, which should
                be loaded into the datafusion session context.
        Returns:
            None
        """
        if len(named_tables) > 0:
            for table_name, file_path in named_tables.items():
                if self._ctx.table_exist(table_name):
                    self._ctx.deregister_table(table_name)
                self._ctx.register_parquet(table_name, file_path)
                assert self._ctx.table_exist(table_name)
        else:
            if not self._ctx.table_exist("t"):
                named_tables = pa.RecordBatch.from_arrays(
                    [
                        pa.array(COLUMN_A),
                        pa.array(COLUMN_B),
                        pa.array(COLUMN_C),
                        pa.array(COLUMN_D),
                    ],
                    names=["a", "b", "c", "d"],
                )
                self._ctx.register_record_batches("t", [[named_tables]])

    def name(self):
        return "DataFusionProducer"
