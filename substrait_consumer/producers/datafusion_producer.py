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

    def set_db_connection(self, db_connection):
        self._db_connection = db_connection

    def produce_substrait(self, sql_query: str, validate = False, ibis_expr: str = None) -> str:
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

    def register_tables(self, created_tables, file_names):
        """
        Register tables to the datafusion session context.

        Parameters:
            created_tables:
                The set of tables that have already been created.
            file_names:
                Name of parquet files.
        Returns:
            None
        """
        if len(file_names) > 0:
            parquet_file_paths = SubstraitUtils.get_full_path(file_names)
            for file_name, file_path in zip(file_names, parquet_file_paths):
                table_name = Path(file_name).stem
                if not self._ctx.table_exist(table_name):
                    created_tables.add(table_name)
                    self._ctx.register_parquet(table_name, file_path)
        else:
            if not self._ctx.table_exist("t"):
                tables = pa.RecordBatch.from_arrays(
                    [
                        pa.array(COLUMN_A),
                        pa.array(COLUMN_B),
                        pa.array(COLUMN_C),
                        pa.array(COLUMN_D),
                    ],
                    names=["a", "b", "c", "d"],
                )
                self._ctx.register_record_batches("t", [[tables]])

    def format_sql(self, created_tables, sql_query, file_names):
        self.register_tables(created_tables, file_names)
        if len(file_names) > 0:
            table_names = [Path(f).stem for f in file_names]
            sql_query = sql_query.format(*table_names)
        return sql_query

    def name(self):
        return "DataFusionProducer"
