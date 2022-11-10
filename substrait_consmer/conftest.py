from pathlib import Path

import duckdb
import pytest

from filelock import FileLock
from tests.consumers import AceroConsumer, DuckDBConsumer
from tests.producers import DuckDBProducer, IbisProducer


@pytest.fixture(scope="session")
def prepare_tpch_parquet_data(scale_factor=0.1):
    """
    Generate TPCH data to be used for testing. Data is generated in tests/data/tpch_parquet

    Parameters:
        scale_factor:
            Scale factor for TPCH data generation.
    """
    data_path = Path(__file__).parent / "data" / "tpch_parquet"
    data_path.mkdir(parents=True, exist_ok=True)
    lock_file = data_path / "data.json"
    with FileLock(str(lock_file) + ".lock"):
        con = duckdb.connect()
        con.execute(f"CALL dbgen(sf={scale_factor})")
        con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT PARQUET);")


def pytest_addoption(parser):
    parser.addoption(
        "--consumer",
        action="store",
        default=",".join([x.__name__ for x in CONSUMERS]),
        help=f"A comma separated list of consumers to run against.",
        choices=[x.__name__ for x in CONSUMERS]
    )
    parser.addoption(
        "--producer",
        action="store",
        default=",".join([x.__name__ for x in PRODUCERS]),
        help="A comma separated list of producers to run against.",
        choices=[x.__name__ for x in PRODUCERS]
    )


PRODUCERS = [DuckDBProducer, IbisProducer]
CONSUMERS = [AceroConsumer, DuckDBConsumer]


def _get_consumers():
    return [cls for cls in CONSUMERS]


def _get_producers():
    return [cls for cls in PRODUCERS]


@pytest.fixture(params=_get_consumers(), scope="session")
def consumer(request):
    consumers_list = request.config.option.consumer.split(",")
    consumer = request.param()
    if type(consumer).__name__ in consumers_list:
        return consumer
    else:
        pytest.skip(
            f"Skipping consumer: '{type(consumer).__name__})', "
            f"the specified consumers are: {consumers_list}"
        )


@pytest.fixture(params=_get_producers(), scope="session")
def producer(request):
    producers_list = request.config.option.producer.split(",")
    producer = request.param()
    if type(producer).__name__ in producers_list:
        return producer
    else:
        pytest.skip(
            f"Skipping producer: '{type(producer).__name__})', "
            f"the specified producers are: {producers_list}"
        )
