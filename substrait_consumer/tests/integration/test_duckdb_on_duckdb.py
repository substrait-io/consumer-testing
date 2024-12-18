from pathlib import Path

import duckdb
import pytest
from pytest_snapshot.plugin import Snapshot

from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer
from substrait_consumer.functional.common import substrait_consumer_sql_test
from substrait_consumer.functional.utils import load_json
from substrait_consumer.producers.duckdb_producer import DuckDBProducer

PLAN_DIR = Path(__file__).parent.parent / "functional" / "tpch"

CONFIG_DIR = Path(__file__).parent.parent
TPCH_CONFIG_DIR = CONFIG_DIR / "integration" / "tpch"
TEST_CASE_PATHS = list(
    (path.relative_to(CONFIG_DIR),) for path in TPCH_CONFIG_DIR.rglob("*.json")
)
IDS = list((str(path[0]).removesuffix(".json") for path in TEST_CASE_PATHS))

TPCH_SNAPSHOT_DIR = (
    Path(__file__).parent.parent / "functional" / "integration" / "tpch_snapshots"
)


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
def test_substrait_query(
    path: Path,
    snapshot: Snapshot,
    db_con: duckdb.DuckDBPyConnection,
    record_property,
) -> None:
    test_case = load_json(CONFIG_DIR / path)
    local_files = test_case["local_files"]
    named_tables = test_case["named_tables"]
    consumer = DuckDBConsumer()
    producer = DuckDBProducer()
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
