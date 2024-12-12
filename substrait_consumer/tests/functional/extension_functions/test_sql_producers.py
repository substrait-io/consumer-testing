from pathlib import Path

import duckdb
import pytest
from pytest_snapshot.plugin import Snapshot

from substrait_consumer.functional.utils import load_json
from substrait_consumer.functional.common import substrait_producer_sql_test

CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "functional"
FUNCTION_CONFIG_DIR = CONFIG_DIR / "function"
TEST_CASE_PATHS = list(
    (path.relative_to(CONFIG_DIR),) for path in FUNCTION_CONFIG_DIR.rglob("*.json")
)
IDS = (str(path[0]).removesuffix(".json") for path in TEST_CASE_PATHS)


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
@pytest.mark.usefixtures("prepare_small_tpch_parquet_data")
@pytest.mark.produce_substrait_snapshot
def test_sql_producer(
    path: Path,
    snapshot: Snapshot,
    record_property,
    db_con: duckdb.DuckDBPyConnection,
    producer,
):
    test_case = load_json(CONFIG_DIR / path)
    local_files = test_case["local_files"]
    named_tables = test_case["named_tables"]
    sql_query = test_case["sql_query"]
    substrait_producer_sql_test(
        path,
        snapshot,
        record_property,
        db_con,
        local_files,
        named_tables,
        sql_query,
        producer,
        validate=False,
    )
