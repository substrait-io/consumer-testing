from __future__ import annotations

import inspect
from pathlib import Path
import types
from typing import TYPE_CHECKING, Any, Callable

import pytest
from duckdb import DuckDBPyConnection

if TYPE_CHECKING:
    from pytest_snapshot.plugin import Snapshot

from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.ibis_producer import IbisProducer


FUNCTION_SNAPSHOT_DIR = (
    Path(__file__).parent.parent / "tests" / "functional" / "extension_functions"
)
RELATION_SNAPSHOT_DIR = (
    Path(__file__).parent.parent / "tests" / "functional" / "relations"
)
INTEGRATION_SNAPSHOT_DIR = (
    Path(__file__).parent.parent / "tests" / "functional" / "integration"
)
SNAPSHOT_DIR = {
    "function": FUNCTION_SNAPSHOT_DIR,
    "relation": RELATION_SNAPSHOT_DIR,
    "integration": INTEGRATION_SNAPSHOT_DIR,
}


def check_match(
    snapshot: Snapshot, value: str | bytes, snapshot_name: str | Path
) -> bool:
    """
    Returns True iff the given `value` matches the snapshot according to `assert_match`.

    Calls `snapshot.assert_match(value, snapshot_name)` and returns True if the
    snapshot matches, returns False if it doesn't, and reraises any other exception
    that may occur.

    Parameters:
        snapshot:
            `snapshot` test fixture from pytest-snapshot
        value:
            Value to check against the snapshot
        snapshot_name:
            Path to the file containing the snapshot

    Returns:
        True if `value` matches the content of the file in `snapshot_name`, False
        otherwise.
    """
    try:
        snapshot.assert_match(value, snapshot_name)
    except AssertionError as e:
        if str(e).startswith("value does not match the expected value in"):
            return False
        raise e

    return True


def check_subtrait_function_names(
    substrait_plan: dict,
    expected_function_name,
):
    """
    Validate that the produced substrait plans are using the correct names for the
    substrait functions.

    Parameters:
        substrait_plan:
            Substrait Plan
        expected_function_name:
            Name of function as defined in substrait
    """
    functions_list = set()

    for function in substrait_plan["extensions"]:
        function_name = function["extensionFunction"]["name"]
        functions_list.add(function_name)

    # Verify that the proper substrait function name appears in the list of
    # extensionFunctions of the substrait plan
    assert (
        expected_function_name in functions_list
    ), f"Error in function: {expected_function_name}.  Does not appear in {functions_list}."


def generate_snapshot_results(
    path: Path,
    snapshot: Snapshot,
    record_property,
    db_con: DuckDBPyConnection,
    local_files: dict[str, str],
    named_tables: dict[str, str],
    sql_query: tuple,
):
    """
    Generate a "golden" results snapshot using DuckDB.

    Parameters:
        test_name:
            Extension function name and grouping used to determine the folder locations
            of snapshot files.
        snapshot:
            Pytest snapshot plugin used for verification.
        db_con:
            DuckDB connection for creating in memory tables.
        local_files:
            A `dict` mapping format argument names to local files paths.
        named_tables:
            A `dict` mapping table names to local file paths.
        sql_query:
            SQL query.
    """
    producer = DuckDBProducer()
    producer.setup(db_con, local_files, named_tables)

    path = str(path).split(".")[0].split("/")
    category, group, name = path[0], path[1], path[-1]
    record_property("category", category)
    record_property("group", group)
    record_property("name", name)

    outcome_path = f"{name}-generate-outcome.txt"
    data_path = f"{name}_result_data.txt"
    schema_path = f"{name}_result_schema.txt"

    snapshot.snapshot_dir = (
        SNAPSHOT_DIR[category] / (group + "_snapshots") / (category + "_test_results")
    )

    try:
        duckdb_result = producer.run_sql_query(sql_query[0])
    except BaseException as e:
        record_property("outcome", str(type(e)))
        snapshot.assert_match(str(type(e)), outcome_path)
        return

    if duckdb_result is None:
        str_result_schema = "(no result)"
        str_result_data = "(no result)"
    else:
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

    record_property("schema_outcome", str(schema_match_result))
    record_property("data_outcome", str(data_match_result))
    outcome = {
        "schema": schema_match_result,
        "data": data_match_result,
    }
    snapshot.assert_match(str(outcome), outcome_path)


def substrait_producer_ibis_test(category: str, group: str) -> None:
    """
    Decorator for Ibis expression tests.

    This function is meant to be used as a decorator for Ibis expression tests. The
    arguments to the decorator help categorizing the tests. The decorated test case can
    use arbitrary fixtures, which will continue to work. The test case is expected to
    return an Ibis expression. The decorator will wrap the function in a test that (1)
    calls the function to create an Ibis expression, (2) translates that expression to
    Substrait using Ibis' Substrait compiler, and (3) snapshots the resulting plan.

    Parameters:
        category:
            Category of the test ("relation", "function", "integration")
        group:
            Group of the test ("approximation", "arithmetic", "ddl", "join", ...)
    """

    def decorator(
        test_fn: Callable[..., Any],
    ):
        # Actual test logic: create Ibis expression, translate to Substrait, check result.
        def wrapper(snapshot, record_property, *args, **kwargs):
            name = test_fn.__name__
            assert name.startswith("test_") and name.endswith("_expr")
            name = name[len("test_") : -len("_expr")]
            outcome_path = f"{name}-ibis_outcome.txt"
            plan_path = f"{name}_plan.json"

            record_property("category", category)
            record_property("group", group)
            record_property("name", name)
            record_property("producer", "ibis")

            snapshot.snapshot_dir = (
                SNAPSHOT_DIR[category] / (group + "_snapshots") / "IbisProducer"
            )

            ibis_producer = IbisProducer()
            ibis_expr = test_fn(*args, **kwargs)
            try:
                substrait_plan = ibis_producer._produce_substrait(ibis_expr)
            except BaseException as e:
                record_property("outcome", str(type(e)))
                snapshot.assert_match(str(type(e)), outcome_path)
                return

            match_result = check_match(snapshot, str(substrait_plan), plan_path)
            record_property("outcome", str(match_result))
            snapshot.assert_match(str(match_result), outcome_path)

        # Decorator magic: add function parameters for other fixtures.
        signature = inspect.signature(test_fn)
        parameters = list(signature.parameters.values())
        parameters.append(
            inspect.Parameter("snapshot", inspect.Parameter.POSITIONAL_OR_KEYWORD)
        )
        parameters.append(
            inspect.Parameter(
                "record_property", inspect.Parameter.POSITIONAL_OR_KEYWORD
            )
        )
        new_signature = signature.replace(parameters=parameters)
        updated_wrapper = types.FunctionType(
            wrapper.__code__,
            wrapper.__globals__,
            name=test_fn.__name__,
            argdefs=test_fn.__defaults__,
            closure=wrapper.__closure__,
        )
        updated_wrapper.__signature__ = new_signature

        # Add marker and return.
        return pytest.mark.produce_substrait_snapshot(updated_wrapper)

    return decorator


def substrait_producer_sql_test(
    path: Path,
    snapshot: Snapshot,
    record_property,
    db_con: DuckDBPyConnection,
    local_files: dict[str, str],
    named_tables: dict[str, str],
    sql_query: tuple,
    producer,
    validate=False,
):
    """
    Verify the substrait plan produced for the specified function matches up with the
    expected substrait plan snapshot.

    Parameters:
        test_name:
            Extension function name and grouping used to determine the folder locations
            of snapshot files.
        snapshot:
            Pytest snapshot plugin used for verification.
        db_con:
            DuckDB connection for creating in memory named_tables.
        local_files:
            A `dict` mapping format argument names to local files paths.
        named_tables:
            A `dict` mapping table names to local file paths.
        sql_query:
            SQL query.
        producer:
            Substrait producer class.
    """
    if isinstance(producer, IbisProducer):
        pytest.skip("Ibis producer cannot run SQL test")

    sql_query, supported_producers = sql_query

    if not producer.name() in supported_producers:
        pytest.xfail(
            f"{producer.name()} does not support the following SQL: {sql_query}"
        )

    producer.setup(db_con, local_files, named_tables)

    path = str(path).split(".")[0].split("/")
    category, group, name = path[0], path[1], path[-1]
    record_property("category", category)
    record_property("group", group)
    record_property("name", name)
    record_property("producer", producer.name())

    outcome_path = f"{name}-{producer.name()}_outcome.txt"
    plan_path = f"{name}_plan.json"
    snapshot.snapshot_dir = (
        SNAPSHOT_DIR[category] / (group + "_snapshots") / type(producer).__name__
    )

    # Convert the SQL query to a Substrait query plan.
    try:
        substrait_plan = producer.produce_substrait(sql_query, validate)
    except BaseException as e:
        record_property("outcome", str(type(e)))
        snapshot.assert_match(str(type(e)), outcome_path)
        return

    match_result = check_match(snapshot, str(substrait_plan), plan_path)
    record_property("outcome", str(match_result))
    snapshot.assert_match(str(match_result), outcome_path)


def substrait_consumer_sql_test(
    path: Path,
    snapshot: Snapshot,
    record_property,
    db_con: DuckDBPyConnection,
    local_files: dict[str, str],
    named_tables: dict[str, str],
    producer,
    consumer,
):
    """
    Iterate through the substrait plan snapshots from each producer and verify the
    consumer is able to run them and generate results matching the results snapshots.

    Parameters:
        test_name:
            Extension function name and grouping used to determine the folder locations
            of snapshot files.
        snapshot:
            Pytest snapshot plugin used for verification.
        db_con:
            DuckDB connection for creating in memory tables.
        local_files:
            A `dict` mapping format argument names to local files paths.
        named_tables:
            A `dict` mapping table names to local file paths.
        producer:
            Substrait producer class.
        consumer:
            Substrait consumer class.
    """
    consumer.setup(db_con, local_files, named_tables)

    path = str(path).split(".")[0].split("/")
    category, group, name = path[0], path[1], path[-1]
    record_property("category", category)
    record_property("group", group)
    record_property("name", name)
    record_property("producer", producer.name())
    record_property("consumer", consumer.name())

    snapshot.snapshot_dir = (
        SNAPSHOT_DIR[category] / (group + "_snapshots") / (category + "_test_results")
    )
    snapshot_dir = SNAPSHOT_DIR[category]
    plan_path = (
        snapshot_dir
        / (group + "_snapshots")
        / type(producer).__name__
        / f"{name}_plan.json"
    )
    outcome_path = f"{name}-{producer.name()}-{consumer.name()}_outcome.txt"
    data_path = f"{name}_result_data.txt"
    schema_path = f"{name}_result_schema.txt"

    if not plan_path.is_file():
        pytest.skip(f"No substrait plan exists for {producer.name()}:{name}")

    substrait_plan = plan_path.read_text()

    try:
        actual_result = consumer.run_substrait_query(substrait_plan)
    except BaseException as e:
        record_property("outcome", str(type(e)))
        snapshot.assert_match(str(type(e)), outcome_path)
        return

    actual_result = actual_result.rename_columns(
        list(map(str.lower, actual_result.column_names))
    )

    str_schema = str(actual_result.schema)
    schema_match_result = check_match(snapshot, str_schema, schema_path)
    data_list = []
    for column in actual_result.columns:
        data_list.extend(column.data)
        data_list.extend([" "])
    str_data = "\n".join(map(str, data_list))
    data_match_result = check_match(snapshot, str_data, data_path)

    record_property("schema_outcome", str(schema_match_result))
    record_property("data_outcome", str(data_match_result))
    outcome = {
        "schema": schema_match_result,
        "data": data_match_result,
    }
    snapshot.assert_match(str(outcome), outcome_path)


def load_custom_duckdb_table(db_connection):
    db_connection.execute("create table t (a BIGINT, b BIGINT, c boolean, d boolean)")
    db_connection.execute(
        "INSERT INTO t VALUES "
        "(1, 1, TRUE, TRUE), (2, 1, FALSE, TRUE), (3, 1, TRUE, TRUE), "
        "(-4, 1, TRUE, TRUE), (5, 1, FALSE, TRUE), (-6, 2, TRUE, TRUE), "
        "(7, 2, FALSE, TRUE), (8, 2, TRUE, TRUE), (9, 2, FALSE, TRUE), "
        "(NULL, 2, FALSE, TRUE);"
    )
