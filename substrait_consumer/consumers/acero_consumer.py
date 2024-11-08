from __future__ import annotations

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.substrait as substrait

from .consumer import COLUMN_A, COLUMN_B, COLUMN_C, COLUMN_D, Consumer


class AceroConsumer(Consumer):
    """
    Adapts the Acero Substrait consumer to the test framework.
    """

    def __init__(self):
        self.named_tables = {}
        self.table_provider = lambda names, schema: self.named_tables[names[0].lower()]

    def _setup(
        self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]
    ):
        for table_name, file_path in named_tables.items():
            self.named_tables[table_name] = pq.read_table(file_path)
        else:
            table = pa.table(
                {
                    "a": COLUMN_A,
                    "b": COLUMN_B,
                    "c": COLUMN_C,
                    "d": COLUMN_D,
                }
            )
            self.named_tables["t"] = table

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
