from pathlib import Path
import substrait_validator as sv

import jpype.imports

REPO_DIR = Path(__file__).parent.parent


schema_file = Path.joinpath(REPO_DIR, "substrait_consumer/data/tpch_parquet/schema.sql")


def produce_isthmus_substrait(sql_string, schema_list, validate=False):
    """
    Produce the substrait plan using Isthmus.

    Parameters:
        sql_string:
            SQL query.
        schema_list:
            List of schemas.
        validate:
            Validate the Substrait plan.

    Returns:
        Substrait plan in json format.
    """
    from com.google.protobuf.util import JsonFormat as json_formatter

    import substrait_consumer.java_definitions as java

    sql_to_substrait = java.SqlToSubstraitClass()
    java_sql_string = jpype.java.lang.String(sql_string)
    plan = sql_to_substrait.execute(java_sql_string, schema_list)
    json_plan = json_formatter.printer().print_(plan)
    if validate:
        config = sv.Config()
        config.override_diagnostic_level(1002, "warning", "info") # error
        config.override_diagnostic_level(2001, "warning", "info") # warning
        config.override_diagnostic_level(3005, "warning", "info") # warning
        config.override_diagnostic_level(1, "warning", "info") # warning
        sv.check_plan_valid(json_plan, config)
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
    import substrait_consumer.java_definitions as java

    arr = java.ArrayListClass()
    if file_names:
        text_schema_file = open(schema_file)
        schema_string = text_schema_file.read().replace("\n", " ").split(";")[:-1]
        for create_table in schema_string:
            if "small" not in file_names[0]:
                create_table = create_table.replace("_small", "")
            java_obj = jpype.JObject @ jpype.JString(create_table)
            arr.add(java_obj)
    java_obj = jpype.JObject @ jpype.JString(
        "CREATE TABLE T(a integer, b integer, c boolean, d boolean)"
    )
    arr.add(java_obj)

    return java.ListClass @ arr
