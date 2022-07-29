from abc import ABC
from abc import abstractmethod

from pyarrow.lib import Table


class AbstractConsumer(ABC):
    """
    Abstract class for representing a substrait consumer
    """
    @abstractmethod
    def run_substrait_query(self, substrait_query: str) -> pa.Table:
        pass
