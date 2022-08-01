from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.substrait as substrait


class AceroConsumer:
    """
    Implementation of the acero substrait consumer Class for testing
    """

    @staticmethod
    def run_substrait_query(substrait_query: str) -> pa.Table:
        """
        Run the given substrait query and return the result

        Parameters:
            substrait_query: A json formatted string representing the
            substrait query plan

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """
        buf = pa._substrait._parse_json_plan(substrait_query)
        reader = substrait.run_query(buf)
        result = reader.read_all()

        return result

    @staticmethod
    def write_to_arrow_binary_file(table: pa.Table, filepath: Path | str) -> None:
        """
        Given a pyarrow table and filepath, write the table into the file using
        arrow IPC file format.

        Parameters:
            table:
                A pyarrow Table instance
            filepath:
                Path
        """
        with pa.ipc.RecordBatchFileWriter(filepath, schema=table.schema) as writer:
            writer.write_table(table)
