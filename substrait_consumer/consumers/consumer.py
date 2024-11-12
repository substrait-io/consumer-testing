from __future__ import annotations

from abc import ABC, abstractmethod

import pyarrow as pa

from substrait_consumer.common import SubstraitUtils


COLUMN_A = [1, 2, 3, -4, 5, -6, 7, 8, 9, None]
COLUMN_B = [1, 1, 1, 1, 1, 2, 2, 2, 2, 2]
COLUMN_C = [
    True,
    False,
    True,
    True,
    False,
    True,
    False,
    True,
    False,
    False,
]
COLUMN_D = [
    True,
    True,
    True,
    True,
    True,
    True,
    True,
    True,
    True,
    True,
]


class Consumer(ABC):

    def setup(
        self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]
    ):
        """
        Initializes this `Consumer` instance.

        In particular, expands the paths in `local_files` and `named_tables` to
        absolute paths and forwards the arguments to `self._setup` implemented
        by classes inheriting from `Consumer`.

        Parameters:
            db_connection:
                DuckDB connection for this `Consumer`.
            local_files:
                A `dict` mapping format argument names to local files paths.
            named_tables:
                A `dict` mapping table names to local file paths.
        """
        local_files = SubstraitUtils.compute_full_paths(local_files)
        named_tables = SubstraitUtils.compute_full_paths(named_tables)
        self._setup(db_connection, local_files, named_tables)

    @abstractmethod
    def _setup(
        self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]
    ):
        pass

    @abstractmethod
    def run_substrait_query(self, substrait_query: str) -> pa.Table:
        pass
