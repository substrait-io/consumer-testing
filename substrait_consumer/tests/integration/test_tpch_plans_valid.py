from pathlib import Path

import duckdb
import pytest
from pytest_snapshot.plugin import Snapshot

from substrait_consumer.functional.common import check_match
from substrait_consumer.functional.utils import load_json
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer

PLAN_SNAPSHOT_DIR = (
    Path(__file__).parent / "queries" / "tpch_substrait_plans"
)

CONFIG_DIR = Path(__file__).parent.parent / "integration"
TPCH_CONFIG_DIR = CONFIG_DIR / "tpch"
TEST_CASE_PATHS = list(
    (path.relative_to(CONFIG_DIR),) for path in TPCH_CONFIG_DIR.rglob("*.json")
)
IDS = list((str(path[0]).removesuffix(".json") for path in TEST_CASE_PATHS))


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
def test_isthmus_substrait_plan_generation(
    path: Path,
    snapshot: Snapshot,
    db_con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Generate the substrait plans using Isthmus.
    """
    test_case = load_json(CONFIG_DIR / path)
    test_name = test_case["test_name"]
    local_files = test_case["local_files"]
    named_tables = test_case["named_tables"]
    sql_query, supported_producers = test_case["sql_query"]

    assert "isthmus" in supported_producers

    tpch_num = test_name.split("_")[-1].zfill(2)
    snapshot.snapshot_dir = PLAN_SNAPSHOT_DIR

    producer = IsthmusProducer()
    producer.setup(db_con, local_files, named_tables)

    try:
        substrait_query = producer.produce_substrait(sql_query)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"query_{tpch_num}_outcome.txt")
        return

    match_result = check_match(
        snapshot, str(substrait_query), f"query_{tpch_num}_plan.json"
    )
    snapshot.assert_match(str(match_result), f"query_{tpch_num}_outcome.txt")


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
def test_isthmus_substrait_plans_valid(
    path: Path,
    snapshot: Snapshot,
    db_con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Run the Isthmus generated substrait plans through the substrait validator.

    Parameters:
        substrait_query:
            Substrait query.
    """
    test_case = load_json(CONFIG_DIR / path)
    test_name = test_case["test_name"]
    local_files = test_case["local_files"]
    named_tables = test_case["named_tables"]
    sql_query, supported_producers = test_case["sql_query"]
    tpch_num = int(test_name.split("_")[-1])

    assert "isthmus" in supported_producers

    snapshot.snapshot_dir = snapshot.snapshot_dir.parent / f"test_tpch_sql_{tpch_num}"

    producer = IsthmusProducer()
    producer.setup(db_con, local_files, named_tables)

    try:
        producer.produce_substrait(sql_query, validate=True)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"test_tpch_sql_{tpch_num}_outcome.txt")
        return

    snapshot.assert_match("True", f"test_tpch_sql_{tpch_num}_outcome.txt")


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
def test_duckdb_substrait_plans_valid(
    path: Path,
    snapshot: Snapshot,
    db_con: duckdb.DuckDBPyConnection,
) -> None:
    """
    Run the Duckdb generated substrait plans through the substrait validator.

    Parameters:
        local_files:
            A `dict` mapping format argument names to local files paths.
        named_tables:
            A `dict` mapping table names to local file paths.
        sql_query:
            SQL query.
    """
    test_case = load_json(CONFIG_DIR / path)
    test_name = test_case["test_name"]
    local_files = test_case["local_files"]
    named_tables = test_case["named_tables"]
    sql_query, supported_producers = test_case["sql_query"]
    tpch_num = int(test_name.split("_")[-1])

    assert "duckdb" in supported_producers

    snapshot.snapshot_dir = snapshot.snapshot_dir.parent / f"test_tpch_sql_{tpch_num}"

    # Format the sql query by inserting all the table names
    duckdb_producer = DuckDBProducer()
    duckdb_producer.setup(db_con, local_files, named_tables)

    try:
        duckdb_producer.produce_substrait(sql_query, validate=True)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"test_tpch_sql_{tpch_num}_outcome.txt")
        return

    snapshot.assert_match("True", f"test_tpch_sql_{tpch_num}_outcome.txt")
