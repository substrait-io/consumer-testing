import duckdb
import pytest

from substrait_consumer.functional.filter_relation_configs import (
    FILTER_RELATION_TESTS)
from substrait_consumer.functional.common import (
    generate_snapshot_results,
    substrait_consumer_sql_test,
)
from substrait_consumer.parametrization import custom_parametrization


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestfilterRelation:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait filter relations.
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

    @custom_parametrization(FILTER_RELATION_TESTS)
    @pytest.mark.consume_substrait_snapshot
    def test_consumer_filter_relations(
        self,
        snapshot,
        record_property,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
        producer,
        consumer,
    ) -> None:
        test_name = f"relation:filter:{test_name}"
        substrait_consumer_sql_test(
            test_name,
            snapshot,
            record_property,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
            producer,
            consumer,
        )

    @custom_parametrization(FILTER_RELATION_TESTS)
    @pytest.mark.generate_function_snapshots
    def test_generate_filter_relation_results(
        self,
        snapshot,
        record_property,
        test_name: str,
        local_files: dict[str, str],
        named_tables: dict[str, str],
        sql_query: tuple,
    ) -> None:
        test_name = f"relation:filter:{test_name}"
        generate_snapshot_results(
            test_name,
            snapshot,
            record_property,
            self.db_connection,
            local_files,
            named_tables,
            sql_query,
        )
