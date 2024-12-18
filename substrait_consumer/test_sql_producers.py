from pathlib import Path

import duckdb
import pytest
from pytest_snapshot.plugin import Snapshot

from substrait_consumer.functional.utils import load_json
from substrait_consumer.functional.common import substrait_producer_sql_test

CONFIG_DIR = Path(__file__).parent / "testdata"
TEST_CASE_PATHS = list(
    (path.relative_to(CONFIG_DIR),) for path in CONFIG_DIR.rglob("*.json")
)
IDS = list(str(path[0]).removesuffix(".json") for path in TEST_CASE_PATHS)


@pytest.fixture
def mark_producer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    path = request.getfixturevalue("path")
    if "relation/read/duckdb_read_local_file" in str(path):
        pytest.skip(
            reason="'duckdb_read_local_file' contains an absolute path, which we can't deal with yet"
        )


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
@pytest.mark.usefixtures("prepare_small_tpch_parquet_data")
@pytest.mark.usefixtures("mark_producer_tests_as_xfail")
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


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
@pytest.mark.usefixtures("prepare_small_tpch_parquet_data")
def test_sql_producer_valid(
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
    # Workaround to distinguish validity test from generation test:
    path = Path(str(path).removesuffix(".json") + "-validate")
    substrait_producer_sql_test(
        path,
        snapshot,
        record_property,
        db_con,
        local_files,
        named_tables,
        sql_query,
        producer,
        validate=True,
    )
