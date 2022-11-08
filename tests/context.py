import os
from pathlib import Path

import jpype
import jpype.imports
from jpype.types import *

REPO_DIR = Path(__file__).parent.parent
schema_file = Path.joinpath(REPO_DIR, "tests/data/tpch_parquet/schema.sql")

calcite_jars = Path.joinpath(REPO_DIR, "jars/*")

the_java_home = "CONDA_PREFIX"
if "JAVA_HOME" in os.environ:
    the_java_home = "JAVA_HOME"

java_home_path = os.environ[the_java_home]
jvm_path = java_home_path

if not os.path.isfile(jvm_path):
    jvm_path = java_home_path + "/lib/libjli.dylib"

jpype.startJVM("--enable-preview", convertStrings=True, jvmpath=jvm_path)
jpype.addClassPath(calcite_jars)
from com.google.protobuf.util import JsonFormat

ArrayListClass = jpype.JClass("java.util.ArrayList")
ListClass = jpype.JClass("java.util.List")
SqlToSubstraitClass = jpype.JClass("io.substrait.isthmus.SqlToSubstrait")
json_formatter = JsonFormat


def produce_isthmus_substrait(sql_string, schema_list):
    """
    Produce the substrait plan using Isthmus.

    Parameters:
        sql_string:
            SQL query.
        schema_list:
            List of schemas.

    Returns:
        Substrait plan in json format.
    """
    sql_to_substrait = SqlToSubstraitClass()
    java_sql_string = jpype.java.lang.String(sql_string)
    plan = sql_to_substrait.execute(java_sql_string, schema_list)
    json_plan = json_formatter.printer().print_(plan)
    return json_plan


def get_schema(file_names):
    """
    Create the list of schemas based on the given file names.  If there are no files
    give, a custom schema for the data is used.

    Parameters:
        file_names: List of file names.

    Returns:
        List of all schemas as a java list.
    """
    arr = ArrayListClass()
    if file_names:
        text_schema_file = open(schema_file)
        schema_string = text_schema_file.read().replace("\n", " ").split(";")[:-1]
        for create_table in schema_string:
            java_obj = JObject @ JString(create_table)
            arr.add(java_obj)
    else:
        java_obj = JObject @ JString(
            "CREATE TABLE T(a integer, b integer, c boolean, d boolean)"
        )
        arr.add(java_obj)

    return ListClass @ arr
