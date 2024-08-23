from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterable

import pytest
from duckdb import DuckDBPyConnection
from ibis.expr.types.relations import Table

if TYPE_CHECKING:
    from pytest_snapshot.plugin import Snapshot

from substrait_consumer.producers.duckdb_producer import DuckDBProducer

FUNCTION_SNAPSHOT_DIR = (
    Path(__file__).parent.parent / "tests" / "functional" / "extension_functions"
)
RELATION_SNAPSHOT_DIR = (
    Path(__file__).parent.parent / "tests" / "functional" / "relations"
)


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
    created_tables: set,
    file_names: Iterable[str],
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
        created_tables:
            Tables names that have already been created.
        file_names:
            List of parquet files.
        sql_query:
            SQL query.
    """
    # Load the parquet files into DuckDB and return all the table names as a list
    producer = DuckDBProducer()
    producer.set_db_connection(db_con)
    sql_query = producer.format_sql(created_tables, sql_query[0], file_names)

    duckdb_result = db_con.query(f"{sql_query}").arrow()
    duckdb_result = duckdb_result.rename_columns(
        list(map(str.lower, duckdb_result.column_names))
    )
    duckdb_result_data = []
    for column in duckdb_result.columns:
        duckdb_result_data.extend(column.data)
        duckdb_result_data.extend([' '])
    str_result_data = '\n'.join(map(str, duckdb_result_data))
    group, name = test_name.split(":")
    if "relation" in group:
        snapshot.snapshot_dir = RELATION_SNAPSHOT_DIR / group / "relation_test_results"
    else:
        snapshot.snapshot_dir = FUNCTION_SNAPSHOT_DIR / group / "function_test_results"
    snapshot.assert_match(str_result_data, f"{name}_result.txt")


def substrait_producer_sql_test(
    test_name: str,
    snapshot: Snapshot,
    db_con: DuckDBPyConnection,
    created_tables: set,
    file_names: Iterable[str],
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
            DuckDB connection for creating in memory tables.
        created_tables:
            Tables names that have already been created.
        file_names:
            List of parquet files.
        sql_query:
            SQL query.
        ibis_expr:
            Ibis expression.
        producer:
            Substrait producer class.
        *args:
            The data tables to be passed to the ibis expression.
    """
    producer.set_db_connection(db_con)
    supported_producers = sql_query[1]

    # Load the parquet files into DuckDB and return all the table names as a list
    sql_query = producer.format_sql(created_tables, sql_query[0], file_names)

    # Convert the SQL/Ibis expression to a substrait query plan
    if type(producer).__name__ == "IbisProducer":
        if ibis_expr:
            substrait_plan = producer.produce_substrait(sql_query, ibis_expr(*args))
        else:
            pytest.xfail("ibis expression currently undefined")
    else:
        if type(producer) in supported_producers:
            substrait_plan = producer.produce_substrait(sql_query, validate)
        else:
            pytest.xfail(
                f"{type(producer).__name__} does not support the following SQL: "
                f"{sql_query}"
            )

    group, name = test_name.split(":")
    if "relation" in group:
        snapshot.snapshot_dir = RELATION_SNAPSHOT_DIR / group / type(producer).__name__
    else:
        snapshot.snapshot_dir = FUNCTION_SNAPSHOT_DIR / group / type(producer).__name__
    snapshot.assert_match(str(substrait_plan), f"{name}_plan.json")


def substrait_consumer_sql_test(
    test_name: str,
    snapshot: Snapshot,
    db_con: DuckDBPyConnection,
    created_tables: set,
    file_names: Iterable[str],
    sql_query: tuple,
    ibis_expr: Callable[[Table], Table],
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
        created_tables:
            Tables names that have already been created.
        file_names:
            List of parquet files.
        sql_query:
            SQL query.
        ibis_expr:
            Ibis expression.
        producer:
            Substrait producer class.
        consumer:
            Substrait consumer class.
    """
    consumer.setup(db_con, created_tables, file_names)

    group, name = test_name.split(":")
    snopshot_dir = RELATION_SNAPSHOT_DIR if "relation" in group else FUNCTION_SNAPSHOT_DIR
    plan_path = (
        snopshot_dir
        / group
        / type(producer).__name__
        / f"{name}_plan.json"
    )
    if plan_path.is_file():
        substrait_plan = plan_path.read_text()

        results_dir = "relation_test_results" if "relation" in group else "function_test_results"
        snapshot.snapshot_dir = snopshot_dir / group / results_dir
        actual_result = consumer.run_substrait_query(substrait_plan)
        actual_result = actual_result.rename_columns(
            list(map(str.lower, actual_result.column_names))
        ).columns
        result_list = []
        for column in actual_result:
            result_list.extend(column.data)
            result_list.extend([' '])
        str_result = '\n'.join(map(str, result_list))
        snapshot.assert_match(str_result, f"{name}_result.txt")

    else:
        pytest.skip(
            f"No substrait plan exists for {type(producer).__name__}:{name}"
        )


def load_custom_duckdb_table(db_connection):
    db_connection.execute("create table t (a BIGINT, b BIGINT, c boolean, d boolean)")
    db_connection.execute(
        "INSERT INTO t VALUES "
        "(1, 1, TRUE, TRUE), (2, 1, FALSE, TRUE), (3, 1, TRUE, TRUE), "
        "(-4, 1, TRUE, TRUE), (5, 1, FALSE, TRUE), (-6, 2, TRUE, TRUE), "
        "(7, 2, FALSE, TRUE), (8, 2, TRUE, TRUE), (9, 2, FALSE, TRUE), "
        "(NULL, 2, FALSE, TRUE);"
    )
