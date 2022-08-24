from __future__ import annotations

import pyarrow as pa
import pyarrow.substrait as substrait


class AceroConsumer:
    """
    Adapts the Acero Substrait consumer to the test framework.
    """

    @staticmethod
    def run_substrait_query(substrait_query: bytes) -> pa.Table:
        """
        Run the given substrait query and return the result

        Parameters:
            substrait_query: A json formatted byte representation of the
            substrait query plan

        Returns:
            A pyarrow table resulting from running the substrait query plan.
        """
        buf = pa._substrait._parse_json_plan(substrait_query)
        reader = substrait.run_query(buf)
        result = reader.read_all()

        return result
