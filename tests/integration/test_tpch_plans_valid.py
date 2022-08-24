import duckdb
import pytest
import substrait_validator as sv

from ..consumers import DuckDBConsumer
from ..parametrization import custom_parametrization
from .queries.tpch_test_cases import TPCH_QUERY_TESTS


class TestTpchPlansValid:
    """
    Test Class for validating TPC-H substrait plans.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls
        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("INSTALL substrait")
        cls.db_connection.execute("LOAD substrait")
        cls.duckdb_consumer = DuckDBConsumer(cls.db_connection)
        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_isthmus_substrait_plans_valid(
        self,
        test_name: str,
        file_names: list,
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
        # not yet implemented: typecast validation rules are not yet implemented
        config.override_diagnostic_level(1, "warning", "info")
        # function definition unavailable: cannot check validity of call
        config.override_diagnostic_level(6003, "warning", "info")
        # relative URL without a base
        config.override_diagnostic_level(2002, "warning", "info")
        # use of anchor zero is discouraged
        config.override_diagnostic_level(3005, "warning", "info")

        sv.check_plan_valid(substrait_query, config)

    @custom_parametrization(TPCH_QUERY_TESTS)
    def test_duckdb_substrait_plans_valid(
        self,
        test_name: str,
        file_names: list,
        sql_query: str,
        substrait_query: str,
        sort_results: bool = False,
    ) -> None:
        """
        Run the Duckdb generated substrait plans through the substrait validator.

        Parameters:
            file_names:
                List of parquet files.
            sql_query:
                SQL query.
        """
        config = sv.Config()

        # Duckdb plan overrides
        # not yet implemented: typecast validation rules are not yet implemented
        config.override_diagnostic_level(1, "warning", "info")
        # function definition unavailable: cannot check validity of call
        config.override_diagnostic_level(6003, "warning", "info")
        # Function Anchor to YAML file
        config.override_diagnostic_level(3001, "error", "info")
        # too few field names
        config.override_diagnostic_level(4003, "error", "info")

        # Load the parquet files into DuckDB and return all the table names as a list
        table_names = self.duckdb_consumer.load_tables_from_parquet(
            self.created_tables, file_names
        )

        # Format the sql query by inserting all the table names
        sql_query = sql_query.format(*table_names)

        duckdb_substrait_plan = self.db_connection.get_substrait(sql_query)
        proto_bytes = duckdb_substrait_plan.fetchone()[0]
        sv.check_plan_valid(proto_bytes, config)
