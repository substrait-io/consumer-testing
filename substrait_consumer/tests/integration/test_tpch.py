from pathlib import Path

import duckdb
import pytest
from pytest_snapshot.plugin import Snapshot

from substrait_consumer.functional.common import (
    substrait_producer_sql_test,
    generate_snapshot_results,
    substrait_consumer_sql_test,
)
from substrait_consumer.functional.utils import load_json

CONFIG_DIR = Path(__file__).parent.parent.parent / "testdata"
TPCH_CONFIG_DIR = CONFIG_DIR / "integration" / "tpch"
TEST_CASE_PATHS = list(
    (path.relative_to(CONFIG_DIR),) for path in TPCH_CONFIG_DIR.rglob("*.json")
)
IDS = list((str(path[0]).removesuffix(".json") for path in TEST_CASE_PATHS))


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
def test_substrait_query(
    path: Path,
    snapshot: Snapshot,
    db_con: duckdb.DuckDBPyConnection,
    record_property,
    producer,
    consumer,
) -> None:
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


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
@pytest.mark.produce_substrait_snapshot
def test_substrait_plan_generation(
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
def test_substrait_plans_valid(
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


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
def test_generate_snapshot_results(
    path: Path,
    snapshot: Snapshot,
    record_property,
    db_con: duckdb.DuckDBPyConnection,
):
    test_case = load_json(CONFIG_DIR / path)
    local_files = test_case["local_files"]
    named_tables = test_case["named_tables"]
    sql_query = test_case["sql_query"]
    generate_snapshot_results(
        path,
        snapshot,
        record_property,
        db_con,
        local_files,
        named_tables,
        sql_query,
    )
