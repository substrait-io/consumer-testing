from pathlib import Path

import duckdb
import pytest
from filelock import FileLock

from substrait_consumer.consumers.acero_consumer import AceroConsumer
from substrait_consumer.consumers.datafusion_consumer import DataFusionConsumer
from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer
from substrait_consumer.producers.datafusion_producer import DataFusionProducer
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.ibis_producer import IbisProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer


@pytest.fixture(scope="session")
def prepare_tpch_parquet_data():
    """
    Generate TPCH data to be used for testing.

    Parameters:
        scale_factor:
            Scale factor for TPCH data generation.
    """
    prepare_tpch(scale_factor=0.1)


@pytest.fixture(scope="session")
def prepare_small_tpch_parquet_data():
    """
    Generate a small set of TPCH data with to be used for testing.
    """
    prepare_tpch(scale_factor=0.0001, suffix="_small")


def prepare_tpch(scale_factor, suffix=None):
    """
    Common function to generate TPCH data to be used for testing. Data is generated in
    tests/data/tpch_parquet.

    Parameters:
        scale_factor: Scale factor of the TPCH data to be generated.
        suffix: suffix to be added to the parquet file names.
    """
    data_path = Path(__file__).parent / "data" / "tpch_parquet"
    data_path.mkdir(parents=True, exist_ok=True)
    lock_file = data_path / "data.json"
    with FileLock(str(lock_file) + ".lock"):
        con = duckdb.connect()
        if suffix:
            con.execute(f"CALL dbgen(sf={scale_factor}, suffix={suffix})")
        else:
            con.execute(f"CALL dbgen(sf={scale_factor})")
        con.execute(f"EXPORT DATABASE '{data_path}' (FORMAT PARQUET);")


def pytest_addoption(parser):
    parser.addoption(
        "--consumer",
        action="store",
        default=",".join([x.__name__ for x in CONSUMERS]),
        help=f"A comma separated list of consumers to run against.",
        choices=[x.__name__ for x in CONSUMERS],
    )
    parser.addoption(
        "--producer",
        action="store",
        default=",".join([x.__name__ for x in PRODUCERS]),
        help="A comma separated list of producers to run against.",
        choices=[x.__name__ for x in PRODUCERS],
    )
    parser.addoption(
        "--adhoc_producer",
        action="store",
        default="",
        help="A comma separated list of producers to run against.",
        choices=[x.__name__ for x in PRODUCERS],
    )
    parser.addoption(
        "--saveplan",
        action="store",
        default=False,
        help="Save the substrait plans created by each producer.",
    )


@pytest.fixture
def saveplan(request):
    return request.config.getoption("--saveplan")


PRODUCERS = [DataFusionProducer, DuckDBProducer, IbisProducer, IsthmusProducer]
CONSUMERS = [AceroConsumer, DataFusionConsumer, DuckDBConsumer]


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


@pytest.fixture(params=_get_producers(), scope="session")
def adhoc_producer(request):
    producers_list = request.config.option.adhoc_producer.split(",")
    producer = request.param()
    if type(producer).__name__ in producers_list:
        return producer
    else:
        pytest.skip(
            f"Skipping producer: '{type(producer).__name__})', "
            f"the specified producers are: {producers_list}"
        )
