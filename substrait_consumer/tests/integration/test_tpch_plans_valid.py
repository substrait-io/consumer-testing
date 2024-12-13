from pathlib import Path

import duckdb
import pytest
import substrait_validator as sv

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
        sort_results: bool = False,
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
        sort_results: bool = False,
    ) -> None:
        """
        Run the Isthmus generated substrait plans through the substrait validator.

        Parameters:
            substrait_query:
                Substrait query.
        """
        config = sv.Config()
        # Isthmus plan overrides
        # ValueError: Error at plan: missing required protobuf field: version (code 1002)
        config.override_diagnostic_level(1002, "info", "info")
        # ValueError: Warning at plan.extension_uris[0].uri: did not attempt to resolve YAML:
        # configured recursion limit for URI resolution has been reached
        config.override_diagnostic_level(2001, "info", "info")
        config.override_diagnostic_level(3005, "info", "info")  # warning
        # Warning. not yet implemented: matching function calls with their definitions
        config.override_diagnostic_level(1, "info", "info")

        try:
            sv.check_plan_valid(substrait_query, config)
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
        sort_results: bool = False,
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
        config = sv.Config()

        # Duckdb plan overrides
        # not yet implemented: typecast validation rules are not yet implemented
        config.override_diagnostic_level(1, "info", "info")
        # function definition unavailable: cannot check validity of call
        config.override_diagnostic_level(6003, "info", "info")
        # Function Anchor to YAML file
        config.override_diagnostic_level(3001, "info", "info")
        # too few field names
        config.override_diagnostic_level(4003, "info", "info")

        # Format the sql query by inserting all the table names
        self.duckdb_producer.setup(self.db_connection, local_files, named_tables)

        try:
            proto_bytes = self.duckdb_producer.produce_substrait(sql_query)
            # TODO: failures in the validator obstruct an otherwise passing test!
            sv.check_plan_valid(proto_bytes, config)
        except BaseException as e:
            snapshot.assert_match(str(type(e)), f"{test_name}_outcome.txt")
            return

        snapshot.assert_match("True", f"{test_name}_outcome.txt")
