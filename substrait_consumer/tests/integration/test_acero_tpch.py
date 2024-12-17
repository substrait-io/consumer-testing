from pathlib import Path

import duckdb
import pytest
from pytest_snapshot.plugin import Snapshot

from substrait_consumer.consumers.acero_consumer import AceroConsumer
from substrait_consumer.functional.utils import load_json
from substrait_consumer.producers.duckdb_producer import DuckDBProducer

PLAN_DIR = Path(__file__).parent / "queries" / "tpch_substrait_plans"

CONFIG_DIR = Path(__file__).parent.parent / "integration"
TPCH_CONFIG_DIR = CONFIG_DIR / "tpch"
TEST_CASE_PATHS = list(
    (path.relative_to(CONFIG_DIR),) for path in TPCH_CONFIG_DIR.rglob("*.json")
)
IDS = list((str(path[0]).removesuffix(".json") for path in TEST_CASE_PATHS))


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
def test_isthmus_substrait_plan(
    path: Path,
    snapshot: Snapshot,
    db_con: duckdb.DuckDBPyConnection,
) -> None:
    """
    1.  Format the substrait_query by replacing the 'local_files' 'uri_file'
        path with the full path to the parquet data.
    2.  Format the SQL query to work with DuckDB by setting the 'Table'
        Parameters to be the relative files paths for parquet data.
    3.  Run the substrait query plan.
    4.  Execute the SQL on DuckDB.
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
        substrait_query:
            Substrait query.
    """
    test_case = load_json(CONFIG_DIR / path)
    test_name = test_case["test_name"]
    local_files = test_case["local_files"]
    named_tables = test_case["named_tables"]
    sql_query, supported_producers = test_case["sql_query"]

    assert "duckdb" in supported_producers

    tpch_num = int(test_name.split("_")[-1])

    snapshot.snapshot_dir = snapshot.snapshot_dir.parent / f"test_tpch_sql_{tpch_num}"

    substrait_plan_path = PLAN_DIR / f"query_{tpch_num:02d}_plan.json"
    with open(substrait_plan_path, "r") as f:
        substrait_query = f.read()

    duckdb_producer = DuckDBProducer()

    # Format the substrait query to include the parquet file paths.
    # Calculate the result of running the substrait query plan.
    consumer = AceroConsumer()
    consumer.setup(db_con, local_files, named_tables)

    try:
        subtrait_query_result_tb = consumer.run_substrait_query(substrait_query)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"query_{tpch_num:02d}_outcome.txt")
        return

    # Reformat the sql query to be used by duck db by inserting all the
    # parquet filepaths where the table names should be.
    # Calculate results to verify against by running the SQL query on DuckDB
    sql_query = duckdb_producer.format_sql(sql_query)
    try:
        duckdb_query_result_tb = duckdb_producer.run_sql_query(sql_query)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"query_{tpch_num:02d}_outcome.txt")
        return

    col_names = [x.lower() for x in subtrait_query_result_tb.column_names]
    exp_col_names = [x.lower() for x in duckdb_query_result_tb.column_names]

    # Verify results between substrait plan query and sql running against
    # duckdb are equal.
    outcome = {
        "column_names": col_names == exp_col_names,
        "table": subtrait_query_result_tb == duckdb_query_result_tb,
    }
    snapshot.assert_match(str(outcome), f"query_{tpch_num:02d}_outcome.txt")


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
def test_duckdb_substrait_plan(
    path: Path,
    snapshot: Snapshot,
    db_con: duckdb.DuckDBPyConnection,
) -> None:
    """
    1.  Load all the parquet files into DuckDB as separate named_tables.
    2.  Format the SQL query to work with DuckDB by inserting all the table names.
    3.  Execute the SQL on DuckDB.
    4.  Produce the substrait plan with duckdb
    5.  Run the duckdb substrait plan against Acero
    5.  Compare the results of running the duckdb plan on Acero against the results of
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

    snapshot.snapshot_dir = snapshot.snapshot_dir.parent / f"test_tpch_sql_{tpch_num}"

    duckdb_producer = DuckDBProducer()
    acero_consumer = AceroConsumer()

    duckdb_producer.setup(db_con, local_files, named_tables)
    acero_consumer.setup(db_con, local_files, named_tables)

    # Convert the SQL into a substrait query plan
    try:
        proto_bytes = duckdb_producer.produce_substrait(sql_query)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"query_{tpch_num:02d}_outcome.txt")
        return

    # Run the duckdb produced substrait plan against Acero
    try:
        subtrait_query_result_tb = acero_consumer.run_substrait_query(proto_bytes)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"query_{tpch_num:02d}_outcome.txt")
        return

    # Calculate results to verify against by running the SQL query on DuckDB
    try:
        duckdb_sql_result_tb = duckdb_producer.run_sql_query(sql_query)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"query_{tpch_num:02d}_outcome.txt")
        return

    col_names = [x.lower() for x in subtrait_query_result_tb.column_names]
    exp_col_names = [x.lower() for x in duckdb_sql_result_tb.column_names]

    # Verify results between substrait plan query and sql running against
    # duckdb are equal.
    outcome = {
        "column_names": col_names == exp_col_names,
        "table": subtrait_query_result_tb == duckdb_sql_result_tb,
    }
    snapshot.assert_match(str(outcome), f"query_{tpch_num:02d}_outcome.txt")
