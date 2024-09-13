from typing import Callable, Iterable

import duckdb
from ibis.expr.types.relations import Table
from ibis_substrait.tests.compiler.conftest import *

from substrait_consumer.functional.ddl_relation_configs import (
    DDL_RELATION_TESTS)
from substrait_consumer.functional.common import (
    generate_snapshot_results,
    substrait_consumer_sql_test, substrait_producer_sql_test)
from substrait_consumer.parametrization import custom_parametrization


@pytest.fixture
def mark_producer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    test_case_name = request.node.callspec.id.split('-')[-1]
    if test_case_name in ["create_table", "drop_table", "alter_table", "alter_columnn",
                          "drop_columnn", "create_view", "create_or_replace_view"]:
        pytest.skip(reason='Creating substrait plans with write relations is not supported')


@pytest.fixture
def mark_consumer_tests_as_xfail(request):
    """Marks a subset of tests as expected to be fail."""
    test_case_name = request.node.callspec.id.split('-')[-1]
    if test_case_name in ["create_table", "drop_table", "alter_table", "alter_columnn",
                          "drop_columnn", "create_view", "create_or_replace_view"]:
        pytest.skip(reason='Creating substrait plans with write relations is not supported')


@pytest.mark.usefixtures("prepare_tpch_parquet_data")
class TestDDLRelation:
    """
    Test Class verifying different consumers are able to run substrait plans
    that include substrait sort relations.
    """

    @staticmethod
    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(request):
        cls = request.cls

        cls.db_connection = duckdb.connect()
        cls.db_connection.execute("install substrait")
        cls.db_connection.execute("load substrait")
        cls.created_tables = set()

        yield

        cls.db_connection.close()

    @custom_parametrization(DDL_RELATION_TESTS)
    @pytest.mark.produce_substrait_snapshot
    @pytest.mark.usefixtures('mark_producer_tests_as_xfail')
    def test_producer_ddl_relations(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        partsupp
    ) -> None:
        test_name = f"ddl_relation_snapshots:{test_name}"
        substrait_producer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            partsupp,
            validate=True
        )

    @custom_parametrization(DDL_RELATION_TESTS)
    @pytest.mark.consume_substrait_snapshot
    @pytest.mark.usefixtures('mark_consumer_tests_as_xfail')
    def test_consumer_ddl_relations(
        self,
        snapshot,
        test_name: str,
        file_names: Iterable[str],
        sql_query: tuple,
        ibis_expr: Callable[[Table], Table],
        producer,
        consumer,
    ) -> None:
        test_name = f"ddl_relation_snapshots:{test_name}"
        substrait_consumer_sql_test(
            test_name,
            snapshot,
            self.db_connection,
            self.created_tables,
            file_names,
            sql_query,
            ibis_expr,
            producer,
            consumer,
        )
