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
        default=",".join(CONSUMERS.keys()),
        help=f"A comma separated list of consumers to run against.",
        choices=CONSUMERS.keys(),
    )
    parser.addoption(
        "--producer",
        action="store",
        default=",".join(PRODUCERS.keys()),
        help="A comma separated list of producers to run against.",
        choices=PRODUCERS.keys(),
    )
    parser.addoption(
        "--adhoc_producer",
        action="store",
        default="",
        help="A comma separated list of producers to run against.",
        choices=PRODUCERS.keys(),
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


PRODUCERS = {
    cls.name(): cls
    for cls in [DataFusionProducer, DuckDBProducer, IbisProducer, IsthmusProducer]
}
CONSUMERS = {
    cls.name(): cls for cls in [AceroConsumer, DataFusionConsumer, DuckDBConsumer]
}


@pytest.fixture(params=CONSUMERS.keys(), scope="session")
def consumer(request):
    requested_consumers = request.config.option.consumer.split(",")
    consumer_name = request.param
    if consumer_name in requested_consumers:
        return CONSUMERS[consumer_name]()
    else:
        pytest.skip(
            f"Skipping consumer: '{consumer_name})', "
            f"the specified consumers are: {requested_consumers}"
        )


@pytest.fixture(params=PRODUCERS.keys(), scope="session")
def producer(request):
    requested_producers = request.config.option.producer.split(",")
    producer_name = request.param
    if producer_name in requested_producers:
        return PRODUCERS[producer_name]()
    else:
        pytest.skip(
            f"Skipping producer: '{producer_name})', "
            f"the specified producers are: {requested_producers}"
        )


@pytest.fixture(params=PRODUCERS.keys(), scope="session")
def adhoc_producer(request):
    requested_producers = request.config.option.adhoc_producer.split(",")
    producer_name = request.param
    if producer_name in requested_producers:
        return PRODUCERS[producer_name]()
    else:
        pytest.skip(
            f"Skipping producer: '{producer_name})', "
            f"the specified producers are: {requested_producers}"
        )
