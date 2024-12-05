from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable

import pytest
from duckdb import DuckDBPyConnection
from ibis.expr.types.relations import Table

if TYPE_CHECKING:
    from pytest_snapshot.plugin import Snapshot

from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.ibis_producer import IbisProducer

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
    test_name: str,
    snapshot: Snapshot,
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
    # Load the parquet files into DuckDB and return all the table names as a list
    producer = DuckDBProducer()
    producer.setup(db_con, local_files, named_tables)

    duckdb_result = producer.run_sql_query(sql_query[0])
    duckdb_result = duckdb_result.rename_columns(
        list(map(str.lower, duckdb_result.column_names))
    )
    duckdb_result_data = []
    for column in duckdb_result.columns:
        duckdb_result_data.extend(column.data)
        duckdb_result_data.extend([' '])
    str_result_data = '\n'.join(map(str, duckdb_result_data))
    snapshot.assert_match(str_result_data, f"{test_name}_result.txt")


def substrait_producer_sql_test(
    test_name: str,
    snapshot: Snapshot,
    db_con: DuckDBPyConnection,
    local_files: dict[str, str],
    named_tables: dict[str, str],
    sql_query: tuple,
    ibis_expr: Callable[[Table], Table],
    producer,
    *args,
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
        ibis_expr:
            Ibis expression.
        producer:
            Substrait producer class.
        *args:
            The data tables to be passed to the ibis expression.
    """
    producer.setup(db_con, local_files, named_tables)
    sql_query, supported_producers = sql_query

    # Convert the SQL/Ibis expression to a substrait query plan
    if isinstance(producer, IbisProducer):
        if ibis_expr:
            substrait_plan = producer.produce_substrait(sql_query, validate, ibis_expr(*args))
        else:
            pytest.xfail("ibis expression currently undefined")
    else:
        if type(producer) in supported_producers:
            substrait_plan = producer.produce_substrait(sql_query, validate)
        else:
            pytest.xfail(
                f"{producer.name()} does not support the following SQL: {sql_query}"
            )

    plan_path = f"{test_name}-{producer.name()}_plan.json"
    snapshot.assert_match(str(substrait_plan), plan_path)


def substrait_consumer_sql_test(
    test_name: str,
    snapshot: Snapshot,
    db_con: DuckDBPyConnection,
    local_files: dict[str, str],
    named_tables: dict[str, str],
    plan_path: Path,
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
        plan_path:
            Path to the plan that should be executed on the consumer.
        consumer:
            Substrait consumer class.
    """
    consumer.setup(db_con, local_files, named_tables)

    if not plan_path.is_file():
        pytest.skip(f"Substrait plan at '{plan_path}' does not exist")

    substrait_plan = plan_path.read_text()

    actual_result = consumer.run_substrait_query(substrait_plan)
    actual_result = actual_result.rename_columns(
        list(map(str.lower, actual_result.column_names))
    ).columns
    result_list = []
    for column in actual_result:
        result_list.extend(column.data)
        result_list.extend([" "])
    str_result = "\n".join(map(str, result_list))
    snapshot.assert_match(str_result, f"{test_name}_result.txt")


def load_custom_duckdb_table(db_connection):
    db_connection.execute("create table t (a BIGINT, b BIGINT, c boolean, d boolean)")
    db_connection.execute(
        "INSERT INTO t VALUES "
        "(1, 1, TRUE, TRUE), (2, 1, FALSE, TRUE), (3, 1, TRUE, TRUE), "
        "(-4, 1, TRUE, TRUE), (5, 1, FALSE, TRUE), (-6, 2, TRUE, TRUE), "
        "(7, 2, FALSE, TRUE), (8, 2, TRUE, TRUE), (9, 2, FALSE, TRUE), "
        "(NULL, 2, FALSE, TRUE);"
    )
