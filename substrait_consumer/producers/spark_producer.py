from pyspark.sql import SparkSession, DataFrame
from pathlib import Path
import os
import substrait_validator as sv
from .producer import SQLProducer

class SparkProducer(SQLProducer):
    """
    Adapts the Spark Substrait producer to the test framework.
    """

    @classmethod
    def name(self):
        return "spark"

    def __init__(self):
        jars = "io.substrait:core:0.66.0,io.substrait:spark:0.66.0,com.google.protobuf:protobuf-java-util:4.33.0"
        self._spark = SparkSession.builder.master("local").appName("SparkProducer").config("spark.jars.packages", jars).getOrCreate()

    def _setup(
        self, db_connection, local_files: dict[str, str], named_tables: dict[str, str]
    ):
        self._tables = named_tables.keys()

        # since Spark always stores the absolute path name in its filesystem read relation,
        # we need to add a symlink to a fixed temp directory to avoid different path names on
        # different systems.
        temp_dir = "/tmp/substrait-io"
        link = temp_dir + "/consumer-testing"
        Path(temp_dir).mkdir(parents=True, exist_ok=True)
        if os.path.exists(link):
            os.remove(link)
        cwd = os.getcwd()
        os.symlink(cwd, link, target_is_directory=True)

        for name, path  in named_tables.items():
            table = self._spark.read.load(path.replace(cwd, link))
            table.createOrReplaceTempView(name)

    def _produce_substrait(self, sql_query: str, validate=False) -> str:
        """
        Produce the Spark substrait plan using the given SQL query.

        Parameters:
            sql_query:
                SQL query.
        Returns:
            Substrait query plan in json format.
        """

        df = self._spark.sql(sql_query)
        jvm = self._spark.sparkContext._jvm

        sparkPlan = df._jdf.queryExecution().optimizedPlan()

        toSubstrait = jvm.io.substrait.spark.logical.ToSubstraitRel()
        substrait_plan = toSubstrait.convert(sparkPlan)

        proto_converter = jvm.io.substrait.plan.PlanProtoConverter()
        proto_plan = proto_converter.toProto(substrait_plan)
        proto_json = jvm.com.google.protobuf.util.JsonFormat.printer().print(proto_plan)

        if validate:
            config = sv.Config()
            # Warning: cannot automatically determine whether plan version
            # is compatible with the Substrait version
            config.override_diagnostic_level(7, "info", "info")  # warning
            # Warning: did not attempt to resolve YAML: configured recursion
            # limit for URI resolution has been reached
            config.override_diagnostic_level(2001, "info", "info")
            sv.check_plan_valid(proto_json, config)

        return proto_json

    def _format_sql(self, sql_query: str) -> str:
        # The table names are enclosed in single quotes (i.e. string literals)
        # These need to be removed because Spark won't tolerate them.
        sql = sql_query
        for table in ("{"+ t + "}" for t in self._tables):
            sql = sql.replace(f"'{table}'", table)

        return sql
