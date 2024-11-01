from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Iterable

import pyarrow as pa

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
    @abstractmethod
    def setup(self, db_connection, file_names: Iterable[str]):
        pass

    @abstractmethod
    def run_substrait_query(self, substrait_query: str) -> pa.Table:
        pass
