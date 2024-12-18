from pathlib import Path

import duckdb
import pytest
from pytest_snapshot.plugin import Snapshot

from substrait_consumer.functional.utils import load_json
from substrait_consumer.functional.common import substrait_consumer_sql_test
from substrait_consumer.producers.isthmus_producer import IsthmusProducer
from substrait_consumer.consumers.acero_consumer import AceroConsumer

CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "testdata"
RELATION_CONFIG_DIR = CONFIG_DIR / "relation"
TEST_CASE_PATHS = list(
    (path.relative_to(CONFIG_DIR),) for path in RELATION_CONFIG_DIR.rglob("*.json")
)
IDS = (str(path[0]).removesuffix(".json") for path in TEST_CASE_PATHS)


@pytest.fixture
def mark_consumer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    producer = request.getfixturevalue("producer")
    consumer = request.getfixturevalue("consumer")
    path = request.getfixturevalue("path")
    if isinstance(consumer, AceroConsumer):
        if (
            isinstance(producer, IsthmusProducer)
        ) and "compute_within_aggregate" in str(path):
            pytest.skip(
                reason="'isthmus-acero-compute_within_aggregate' currently crashes"
            )


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
@pytest.mark.usefixtures("prepare_small_tpch_parquet_data")
@pytest.mark.usefixtures("mark_consumer_tests_as_xfail")
@pytest.mark.consume_substrait_snapshot
def test_consumer(
    path: Path,
    snapshot: Snapshot,
    record_property,
    db_con: duckdb.DuckDBPyConnection,
    producer,
    consumer,
):
    test_case = load_json(CONFIG_DIR / path)
    local_files = test_case["local_files"]
    named_tables = test_case["named_tables"]
    substrait_consumer_sql_test(
        path,
        snapshot,
        record_property,
        db_con,
        local_files,
        named_tables,
        producer,
        consumer,
    )
