from pathlib import Path

import duckdb
import pytest
from pytest_snapshot.plugin import Snapshot

from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer
from substrait_consumer.functional.common import check_match
from substrait_consumer.functional.utils import load_json
from substrait_consumer.producers.duckdb_producer import DuckDBProducer

PLAN_DIR = Path(__file__).parent.parent / "functional" / "tpch"

CONFIG_DIR = Path(__file__).parent.parent / "integration"
TPCH_CONFIG_DIR = CONFIG_DIR / "tpch"
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
) -> None:
    """
    1.  Load all the parquet files into DuckDB as separate named_tables.
    2.  Format the SQL query to work with DuckDB by inserting all the table names.
    3.  Execute the SQL on DuckDB.
    4.  Run the substrait query plan.
    5.  Compare substrait query plan results against the results of
        running the SQL on DuckDB.

    Parameters:
        test_name:
            Name of test.
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

    assert "duckdb" in supported_producers

    tpch_num = int(test_name.split("_")[-1])

    snapshot.snapshot_dir = TPCH_SNAPSHOT_DIR / "integration_test_results"

    consumer = DuckDBConsumer()
    producer = DuckDBProducer()

    consumer.setup(db_con, local_files, named_tables)
    producer.setup(db_con, local_files, named_tables)

    outcome_path = f"q{tpch_num:02d}-{producer.name()}-{consumer.name()}_outcome.txt"
    data_path = f"q{tpch_num:02d}_result_data.txt"
    schema_path = f"q{tpch_num:02d}_result_schema.txt"

    # Calculate results to verify against by running the SQL query on DuckDB
    try:
        duckdb_result = producer.run_sql_query(sql_query)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), outcome_path)
        return

    duckdb_result = duckdb_result.rename_columns(
        list(map(str.lower, duckdb_result.column_names))
    )
    str_result_schema = str(duckdb_result.schema)
    duckdb_result_data = []
    for column in duckdb_result.columns:
        duckdb_result_data.extend(column.data)
        duckdb_result_data.extend([" "])
    str_result_data = "\n".join(map(str, duckdb_result_data))

    schema_match_result = check_match(snapshot, str_result_schema, schema_path)
    data_match_result = check_match(snapshot, str_result_data, data_path)

    assert schema_match_result
    assert data_match_result

    # Load DuckDB plan from file.
    substrait_plan_path = (
        PLAN_DIR
        / f"q{tpch_num:02d}_snapshots"
        / type(producer).__name__
        / f"q{tpch_num:02d}_plan.json"
    )
    if not substrait_plan_path.is_file():
        pytest.skip(f"No substrait plan exists for {producer.name()}:{test_name}")

    with open(substrait_plan_path, "r") as f:
        proto_bytes = f.read()

    try:
        consumer_result = consumer.run_substrait_query(proto_bytes)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), outcome_path)
        return

    consumer_result = consumer_result.rename_columns(
        list(map(str.lower, consumer_result.column_names))
    )
    str_result_schema = str(consumer_result.schema)
    consumer_result_data = []
    for column in consumer_result.columns:
        consumer_result_data.extend(column.data)
        consumer_result_data.extend([" "])
    str_result_data = "\n".join(map(str, consumer_result_data))

    schema_match_result = check_match(snapshot, str_result_schema, schema_path)
    data_match_result = check_match(snapshot, str_result_data, data_path)

    # Verify results between substrait plan query and sql running against
    # duckdb are equal.
    outcome = {
        "schema": schema_match_result,
        "data": data_match_result,
    }
    snapshot.assert_match(str(outcome), outcome_path)
