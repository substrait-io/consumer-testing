import pyarrow as pa
from pyarrow.lib import Table

from .consumer import AbstractConsumer

try:
    import pyarrow.substrait as substrait
except ImportError:
    substrait = None


class AceroConsumer(AbstractConsumer):
    """
    Implementation of the acero substrait consumer Class for testing
    """
    def __init__(self):
        pass

    @staticmethod
    def run_substrait_query(substrait_query: str) -> type[Table]:
        """
        Run the given substrait query and return the result

        Args:
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
    def write_to_arrow_binary_file(table: type[Table], filepath: str) -> None:
        """
        Given a pyarrow table and filepath, write the table into the file using
        arrow IPC file format.

        Args:
            table: A pyarrow Table instance
            filepath: Path

        Returns:
            None
        """
        with pa.ipc.RecordBatchFileWriter(
                filepath, schema=table.schema) as writer:
            writer.write_table(table)
