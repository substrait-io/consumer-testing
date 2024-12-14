import duckdb
import pytest

from substrait_consumer.functional.aggregate_relation_configs import (
    AGGREGATE_RELATION_TESTS,
)
from substrait_consumer.functional.common import generate_snapshot_results
from substrait_consumer.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_small_tpch_parquet_data")
class TestAggregatetRelation:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait sort relations.
    """

    @staticmethod
    @pytest.fixture(autouse=True)
    def setup_teardown_function(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")

        yield

        cls.db_connection.close()

    @custom_parametrization(AGGREGATE_RELATION_TESTS)
    @pytest.mark.generate_function_snapshots
    def test_generate_aggregate_relation_results(
        self,
        snapshot,
        record_property,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
    ) -> None:
        test_name = f"relation:aggregate:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            record_property,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
        )
