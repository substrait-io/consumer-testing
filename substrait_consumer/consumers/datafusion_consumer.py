from __future__ import annotations

import json
import string
from pathlib import Path
from typing import Iterable

import pyarrow as pa
from datafusion import SessionContext
from datafusion import substrait as ds
from google.protobuf.json_format import Parse
from substrait.gen.proto.plan_pb2 import Plan

from substrait_consumer.common import SubstraitUtils

from .consumer import COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D, Consumer


class DataFusionConsumer(Consumer):
    """
    Adapts the Datafusion Substrait consumer to the test framework.
    """

    def __init__(self):
        self.ctx = SessionContext()

    def setup(self, db_connection, created_tables, file_names: Iterable[str]):
        if len(file_names) > 0:
            parquet_file_paths = SubstraitUtils.get_full_path(file_names)
            for file_name, file_path in zip(file_names, parquet_file_paths):
                table_name = Path(file_name).stem
                table_name = table_name.translate(
                    str.maketrans("", "", string.punctuation)
                )
                if f"{self.__class__.__name__}{table_name}" not in created_tables:
                    created_tables.add(f"{self.__class__.__name__}{table_name}")
                    self.ctx.register_parquet(f"{table_name}", file_path)
        else:
            if not self.ctx.table_exist("t"):
                tables = pa.RecordBatch.from_arrays(
                    [
                        pa.array(COLUMN_A),
                        pa.array(COLUMN_B),
                        pa.array(COLUMN_C),
                        pa.array(COLUMN_D),
                    ],
                    names=["a", "b", "c", "d"],
                )

                self.ctx.register_record_batches("t", [[tables]])

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
        substrait_plan = ds.substrait.serde.deserialize_bytes(plan_bytes)
        logical_plan = ds.substrait.consumer.from_substrait_plan(
            self.ctx, substrait_plan
        )

        # Create a DataFrame from a deserialized logical plan
        df_result = self.ctx.create_dataframe_from_logical_plan(logical_plan)
        return df_result.to_arrow_table()
