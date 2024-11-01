from __future__ import annotations

import string
from pathlib import Path
from typing import Iterable

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.substrait as substrait

from substrait_consumer.common import SubstraitUtils

from .consumer import COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D, Consumer


class AceroConsumer(Consumer):
    """
    Adapts the Acero Substrait consumer to the test framework.
    """

    def __init__(self):
        self.tables = {}
        self.table_provider = lambda names, schema: self.tables[names[0].lower()]

    def setup(self, db_connection, file_names: Iterable[str]):
        if len(file_names) > 0:
            parquet_file_paths = SubstraitUtils.get_full_path(file_names)
            for file_name, file_path in zip(file_names, parquet_file_paths):
                table_name = Path(file_name).stem
                self.tables[table_name] = pq.read_table(file_path)
        else:
            table = pa.table(
                {
                    "a": COLUMN_A,
                    "b": COLUMN_B,
                    "c": COLUMN_C,
                    "d": COLUMN_D,
                }
            )
            self.tables["t"] = table

    def run_substrait_query(self, substrait_query: str) -> pa.Table:
        """
        Run the substrait plan against Acero.

        Parameters:
            substrait_query:
                A substrait plan as a json formatted string.

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """
        substrait_query = pa._substrait._parse_json_plan(substrait_query.encode())

        reader = substrait.run_query(
            substrait_query, table_provider=self.table_provider
        )
        result = reader.read_all()

        return result
