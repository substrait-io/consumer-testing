from __future__ import annotations

import json

import pyarrow as pa
from datafusion import SessionContext
from datafusion import substrait as ds
from google.protobuf.json_format import Parse
from substrait.gen.proto.plan_pb2 import Plan

from .consumer import COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D, Consumer


class DataFusionConsumer(Consumer):
    """
    Adapts the Datafusion Substrait consumer to the test framework.
    """

    def __init__(self):
        self._ctx = SessionContext()

    def _setup(
        self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]
    ):
        for table_name, file_path in named_tables.items():
            if self._ctx.table_exist(table_name):
                self._ctx.deregister_table(table_name)
            self._ctx.register_parquet(table_name, file_path)
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

    def run_substrait_query(self, substrait_query: str) -> pa.Table:
        """
        Run the substrait plan against Datafusion.

        Parameters:
            substrait_query:
                A substrait plan as a json formatted string.

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """
        substrait_json = json.loads(substrait_query)
        plan_proto = Parse(json.dumps(substrait_json), Plan())
        plan_bytes = plan_proto.SerializeToString()
        substrait_plan = ds.serde.deserialize_bytes(plan_bytes)
        logical_plan = ds.consumer.from_substrait_plan(
            self._ctx, substrait_plan
        )

        # Create a DataFrame from a deserialized logical plan
        df_result = self._ctx.create_dataframe_from_logical_plan(logical_plan)
        return df_result.to_arrow_table()
