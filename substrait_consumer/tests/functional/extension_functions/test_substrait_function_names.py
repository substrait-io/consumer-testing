import json
from pathlib import Path

import duckdb
import pytest
from pytest_snapshot.plugin import Snapshot

from substrait_consumer.functional.common import check_subtrait_function_names
from substrait_consumer.functional.utils import load_json
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.ibis_producer import IbisProducer


CONFIG_DIR = Path(__file__).parent.parent.parent.parent / "functional"
FUNCTION_CONFIG_DIR = CONFIG_DIR / "function"
TEST_CASE_PATHS = list(
    (path.relative_to(CONFIG_DIR),) for path in FUNCTION_CONFIG_DIR.rglob("*.json")
)
IDS = (str(path[0]).removesuffix(".json") for path in TEST_CASE_PATHS)


@pytest.mark.parametrize(["path"], TEST_CASE_PATHS, ids=IDS)
@pytest.mark.usefixtures("prepare_tpch_parquet_data")
def test_function_name(
    path: Path,
    snapshot: Snapshot,
    db_con: duckdb.DuckDBPyConnection,
    producer,
):
    """

    Verify the substrait function names that appear in the produced plan match up
    with the function names as defined in Substrait.

    Parameters:
        test_name:
            Expected function name as defined by the substrait spec.
        local_files:
            A `dict` mapping format argument names to local files paths.
        named_tables:
            A `dict` mapping table names to local file paths.
        producer:
            Substrait producer class.
    """
    test_case = load_json(CONFIG_DIR / path)
    local_files = test_case["local_files"]
    named_tables = test_case["named_tables"]
    sql_query = test_case["sql_query"]
    path = str(path).split(".")[0].split("/")
    group, test_name = path[1], path[-1]

    snapshot.snapshot_dir = (
        Path(__file__).parent
        / "snapshots"
        / "test_substrait_function_names"
        / f"test_{group}_function_names"
        / f"{producer.name()}-{test_name}"
    )

    if isinstance(producer, IbisProducer):
        pytest.skip("function names currently not tested for Ibis producer")

    producer.setup(db_con, local_files, named_tables)

    # Grab the json representation of the produced substrait plan to verify
    # the proper substrait function name.
    duckdb_producer = DuckDBProducer(db_con)
    duckdb_producer.setup(db_con, local_files, named_tables)
    try:
        substrait_plan_json = duckdb_producer.produce_substrait(sql_query[0])
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"{test_name}_outcome.txt")
        return

    substrait_plan = json.loads(substrait_plan_json)
    try:
        check_subtrait_function_names(substrait_plan, test_name)
    except BaseException as e:
        snapshot.assert_match(str(type(e)), f"{test_name}_outcome.txt")
        return
