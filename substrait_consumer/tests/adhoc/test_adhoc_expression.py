import json
from pathlib import Path

import duckdb
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.tests.adhoc.ibis_expr import ibis_expr
from substrait_consumer.verification import verify_equals

CUR_DIR = Path(__file__).parent
SQL_FILE_PATH = CUR_DIR / "query.sql"


FILE_NAMES = [
    "customer.parquet",
    "lineitem.parquet",
    "nation.parquet",
    "orders.parquet",
    "part.parquet",
    "partsupp.parquet",
    "region.parquet",
    "supplier.parquet",
]


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestAdhocExpression:
    """
    Test CLI for generating substrait plans from adhoc SQL queries or ibis expressions
    and testing them against different consumers.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls
        cls.produced_plans = set()

    @staticmethod
    @pytest.fixture(scope="function", autouse=True)
    def setup_teardown_function(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")

        yield

        cls.db_connection.close()

    def test_adhoc_expression(
        self,
        adhoc_producer,
        consumer,
        saveplan,
        part,
        supplier,
        partsupp,
        customer,
        orders,
        lineitem,
        nation,
        region,
    ) -> None:
        adhoc_producer.set_db_connection(self.db_connection)
        consumer.setup(self.db_connection, FILE_NAMES)

        with open(SQL_FILE_PATH, "r") as f:
            sql_query = f.read()

        if not sql_query:
            raise ValueError("No SQL query.  Please write SQL into query.sql")
        sql_query = adhoc_producer.format_sql(set(), sql_query, FILE_NAMES)
        substrait_plan = adhoc_producer.produce_substrait(
            sql_query,
            consumer,
            ibis_expr(
                part, supplier, partsupp, customer, orders, lineitem, nation, region
            ),
        )
        producer_name = type(adhoc_producer).__name__
        if isinstance(substrait_plan, str) and saveplan:
            if producer_name not in self.produced_plans:
                self.produced_plans.add(producer_name)
                python_json = json.loads(substrait_plan)
                with open(f"{producer_name}_substrait.json", "w") as outfile:
                    outfile.write(json.dumps(python_json, indent=4))
            else:
                pytest.skip(
                    f"Plan already produced using the producer: {producer_name}"
                )

        actual_result = consumer.run_substrait_query(substrait_plan)
        expected_result = self.db_connection.query(f"{sql_query}").arrow()

        verify_equals(
            actual_result.columns,
            expected_result.columns,
            message=f"Result: {actual_result.columns} "
            f"is not equal to the expected: "
            f"{expected_result.columns}",
        )
