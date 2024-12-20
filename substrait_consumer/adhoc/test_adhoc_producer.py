import json
from pathlib import Path
from typing import Any

import duckdb
from ibis_substrait.tests.compiler.conftest import *

from .ibis_expr import ibis_expr
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.ibis_producer import IbisProducer

SQL_FILE_PATH = Path(__file__).parent / "query.sql"

FILE_NAMES = {
    "customer": "customer.parquet",
    "lineitem": "lineitem.parquet",
    "nation": "nation.parquet",
    "orders": "orders.parquet",
    "part": "part.parquet",
    "partsupp": "partsupp.parquet",
    "region": "region.parquet",
    "supplier": "supplier.parquet",
}


def verify_equals(actual: Any, expected: Any, message: str = "") -> None:
    """
    Verify that 2 objects are equal.  First check to see that object
    types are the same. If they differ, log the objects types and raise
    an error.
    If object types are the same but values are not equal, an error is
    raised and the message is shown.

    Parameters:
        actual:
            Object to evaluate against the expected object.
        expected:
            Object to be evaluated against.
        message:
            Message to be displayed if objects are not equal.
    """
    msg = [f"TEST FAILURE: Verifying equals: {actual} == {expected}."]
    msg = [message] if message else msg

    assert isinstance(actual, type(expected)), (
        f"TEST FAILURE: Object types are not the same. \nActual "
        f"type: {type(actual)}\nExpected type: {type(expected)}"
    )
    assert actual == expected, msg


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestAdhocExpression:
    """
    Test CLI for generating substrait plans from adhoc SQL queries or ibis expressions
    and testing them against different consumers.
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def setup_teardown_function(request):
        cls = request.cls
        cls.produced_plans = set()

    def test_adhoc_sql_query(
        self,
        adhoc_producer,
        consumer,
        saveplan,
        db_con,
        part,
        supplier,
        partsupp,
        customer,
        orders,
        lineitem,
        nation,
        region,
    ) -> None:
        local_files = dict()
        named_tables = FILE_NAMES
        adhoc_producer.setup(db_con, local_files, named_tables)
        consumer.setup(db_con, local_files, named_tables)

        with open(SQL_FILE_PATH, "r") as f:
            sql_query = f.read()

        if not sql_query:
            raise ValueError("No SQL query.  Please write SQL into query.sql")

        if isinstance(adhoc_producer, IbisProducer):
            expr = ibis_expr(
                part, supplier, partsupp, customer, orders, lineitem, nation, region
            )
            substrait_plan = adhoc_producer._produce_substrait(expr)
        else:
            substrait_plan = adhoc_producer.produce_substrait(sql_query)

        producer_name = adhoc_producer.name()
        if isinstance(substrait_plan, str) and saveplan:
            if producer_name not in self.produced_plans:
                self.produced_plans.add(producer_name)
                python_json = json.loads(substrait_plan)
                with open(f"{producer_name}_substrait.json", "w") as outfile:
                    outfile.write(json.dumps(python_json, indent=4))
            else:
                pytest.skip(
                    f"Plan already produced using the producer: {adhoc_producer.name()}"
                )

        actual_result = consumer.run_substrait_query(substrait_plan)
        duckdb_producer = DuckDBProducer()
        duckdb_producer.setup(db_con, local_files, named_tables)
        expected_result = duckdb_producer.run_sql_query(sql_query)

        verify_equals(
            actual_result.columns,
            expected_result.columns,
            message=f"Result: {actual_result.columns} "
            f"is not equal to the expected: "
            f"{expected_result.columns}",
        )
