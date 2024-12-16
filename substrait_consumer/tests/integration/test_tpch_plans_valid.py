from pathlib import Path

import duckdb
import pytest

from pytest_snapshot.plugin import Snapshot

from substrait_consumer.consumers.duckdb_consumer import DuckDBConsumer
from substrait_consumer.functional.common import check_match
from substrait_consumer.parametrization import custom_parametrization
from substrait_consumer.producers.duckdb_producer import DuckDBProducer
from substrait_consumer.producers.isthmus_producer import IsthmusProducer
from .queries.tpch_test_cases import TPCH_QUERY_TESTS

PLAN_SNAPSHOT_DIR = (
    Path(__file__).parent / "queries" / "tpch_substrait_plans"
)


class TestTpchPlansValid:
    """
    Test Class for validating TPC-H substrait plans.
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def setup_teardown_function(request):
        cls = request.cls
        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("INSTALL substrait")
        cls.db_connection.execute("LOAD substrait")
        cls.duckdb_consumer = DuckDBConsumer(cls.db_connection)
        cls.duckdb_producer = DuckDBProducer(cls.db_connection)

        yield

        cls.db_connection.close()

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_isthmus_substrait_plan_generation(
        self,
        test_name: str,
        snapshot: Snapshot,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: str,
        substrait_query: str,
    ) -> None:
        """
        Generate the substrait plans using Isthmus.
        """
        tpch_num = test_name.split("_")[-1].zfill(2)
        snapshot.snapshot_dir = PLAN_SNAPSHOT_DIR

        producer = IsthmusProducer()
        producer.setup(self.db_connection, local_files, named_tables)

        try:
            substrait_query = producer.produce_substrait(sql_query)
        except BaseException as e:
            snapshot.assert_match(str(type(e)), f"query_{tpch_num}_outcome.txt")
            return

        match_result = check_match(
            snapshot, str(substrait_query), f"query_{tpch_num}_plan.json"
        )
        snapshot.assert_match(str(match_result), f"query_{tpch_num}_outcome.txt")

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_isthmus_substrait_plans_valid(
        self,
        test_name: str,
        snapshot: Snapshot,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: str,
        substrait_query: str,
    ) -> None:
        """
        Run the Isthmus generated substrait plans through the substrait validator.

        Parameters:
            substrait_query:
                Substrait query.
        """
        producer = IsthmusProducer()
        producer.setup(self.db_connection, local_files, named_tables)

        try:
            producer.produce_substrait(sql_query, validate=True)
        except BaseException as e:
            snapshot.assert_match(str(type(e)), f"{test_name}_outcome.txt")
            return

        snapshot.assert_match("True", f"{test_name}_outcome.txt")

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_duckdb_substrait_plans_valid(
        self,
        test_name: str,
        snapshot: Snapshot,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: str,
        substrait_query: str,
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

        # Format the sql query by inserting all the table names
        self.duckdb_producer.setup(self.db_connection, local_files, named_tables)

        try:
            self.duckdb_producer.produce_substrait(sql_query, validate=True)
        except BaseException as e:
            snapshot.assert_match(str(type(e)), f"{test_name}_outcome.txt")
            return

        snapshot.assert_match("True", f"{test_name}_outcome.txt")
