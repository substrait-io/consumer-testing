import json
from pathlib import Path
import os

from substrait_consumer.functional.aggregate_relation_configs import AGGREGATE_RELATION_TESTS
from substrait_consumer.functional.ddl_relation_configs import DDL_RELATION_TESTS
from substrait_consumer.functional.fetch_relation_configs import FETCH_RELATION_TESTS
from substrait_consumer.functional.filter_relation_configs import FILTER_RELATION_TESTS
from substrait_consumer.functional.join_relation_configs import JOIN_RELATION_TESTS
from substrait_consumer.functional.project_relation_configs import PROJECT_RELATION_TESTS
from substrait_consumer.functional.read_relation_configs import READ_RELATION_TESTS
from substrait_consumer.functional.set_relation_configs import SET_RELATION_TESTS
from substrait_consumer.functional.sort_relation_configs import SORT_RELATION_TESTS
from substrait_consumer.functional.write_relation_configs import WRITE_RELATION_TESTS

from substrait_consumer.functional.approximation_configs import AGGREGATE_FUNCTIONS as APPROXIMATION_AGGREGATE_FUNCTIONS
from substrait_consumer.functional.arithmetic_configs import SCALAR_FUNCTIONS as ARITHMETIC_SCALAR_FUNCTIONS, AGGREGATE_FUNCTIONS as ARITHMETIC_AGGREGATE_FUNCTIONS
from substrait_consumer.functional.arithmetic_decimal_configs import SCALAR_FUNCTIONS as ARITHMETIC_DECIMAL_SCALAR_FUNCTIONS, AGGREGATE_FUNCTIONS as ARITHMETIC_DECIMAL_AGGREGATE_FUNCTIONS
from substrait_consumer.functional.boolean_configs import SCALAR_FUNCTIONS as BOOLEAN_SCALAR_FUNCTIONS, AGGREGATE_FUNCTIONS as BOOLEAN_AGGREGATE_FUNCTIONS
from substrait_consumer.functional.comparison_configs import SCALAR_FUNCTIONS as COMPARISON_SCALAR_FUNCTIONS
from substrait_consumer.functional.datetime_configs import SCALAR_FUNCTIONS as DATETIME_SCALAR_FUNCTIONS
from substrait_consumer.functional.logarithmic_configs import SCALAR_FUNCTIONS as LOGARITHMIC_SCALAR_FUNCTIONS
from substrait_consumer.functional.rounding_configs import SCALAR_FUNCTIONS as ROUNDING_SCALAR_FUNCTIONS
from substrait_consumer.functional.string_configs import SCALAR_FUNCTIONS as STRING_SCALAR_FUNCTIONS, AGGREGATE_FUNCTIONS as STRING_AGGREGATE_FUNCTIONS

CURRENT_DIR = Path(__file__).parent

AGGREGATE_RELATION_TESTS = [d | {"class": "relation/aggregate" } for d in AGGREGATE_RELATION_TESTS]
DDL_RELATION_TESTS = [d | {"class": "relation/ddl" } for d in DDL_RELATION_TESTS]
FETCH_RELATION_TESTS = [d | {"class": "relation/fetch" } for d in FETCH_RELATION_TESTS]
FILTER_RELATION_TESTS = [d | {"class": "relation/filter" } for d in FILTER_RELATION_TESTS]
JOIN_RELATION_TESTS = [d | {"class": "relation/join" } for d in JOIN_RELATION_TESTS]
PROJECT_RELATION_TESTS = [d | {"class": "relation/project" } for d in PROJECT_RELATION_TESTS]
READ_RELATION_TESTS = [d | {"class": "relation/read" } for d in READ_RELATION_TESTS]
SET_RELATION_TESTS = [d | {"class": "relation/set" } for d in SET_RELATION_TESTS]
SORT_RELATION_TESTS = [d | {"class": "relation/sort" } for d in SORT_RELATION_TESTS]
WRITE_RELATION_TESTS = [d | {"class": "relation/write" } for d in WRITE_RELATION_TESTS]

APPROXIMATION_AGGREGATE_FUNCTIONS = [d | {"class": "function/approximation/aggregate" } for d in APPROXIMATION_AGGREGATE_FUNCTIONS]
ARITHMETIC_AGGREGATE_FUNCTIONS = [d | {"class": "function/arithmetic/aggregate" } for d in ARITHMETIC_AGGREGATE_FUNCTIONS]
ARITHMETIC_DECIMAL_AGGREGATE_FUNCTIONS = [d | {"class": "function/arithmetic_decimal/aggregate" } for d in ARITHMETIC_DECIMAL_AGGREGATE_FUNCTIONS]
BOOLEAN_AGGREGATE_FUNCTIONS = [d | {"class": "function/boolean/aggregate" } for d in BOOLEAN_AGGREGATE_FUNCTIONS]
STRING_AGGREGATE_FUNCTIONS = [d | {"class": "function/string/aggregate" } for d in STRING_AGGREGATE_FUNCTIONS]

ARITHMETIC_SCALAR_FUNCTIONS = [d | {"class": "function/arithmetic/scalar" } for d in ARITHMETIC_SCALAR_FUNCTIONS]
ARITHMETIC_DECIMAL_SCALAR_FUNCTIONS = [d | {"class": "function/arithmetic_decimal/scalar" } for d in ARITHMETIC_DECIMAL_SCALAR_FUNCTIONS]
BOOLEAN_SCALAR_FUNCTIONS = [d | {"class": "function/boolean/scalar" } for d in BOOLEAN_SCALAR_FUNCTIONS]
COMPARISON_SCALAR_FUNCTIONS = [d | {"class": "function/comparison/scalar" } for d in COMPARISON_SCALAR_FUNCTIONS]
DATETIME_SCALAR_FUNCTIONS = [d | {"class": "function/datetime/scalar" } for d in DATETIME_SCALAR_FUNCTIONS]
LOGARITHMIC_SCALAR_FUNCTIONS = [d | {"class": "function/logarithmic/scalar" } for d in LOGARITHMIC_SCALAR_FUNCTIONS]
ROUNDING_SCALAR_FUNCTIONS = [d | {"class": "function/rounding/scalar" } for d in ROUNDING_SCALAR_FUNCTIONS]
STRING_SCALAR_FUNCTIONS = [d | {"class": "function/string/scalar" } for d in STRING_SCALAR_FUNCTIONS]

TESTS = (
    AGGREGATE_RELATION_TESTS +
    DDL_RELATION_TESTS +
    FETCH_RELATION_TESTS +
    FILTER_RELATION_TESTS +
    JOIN_RELATION_TESTS +
    PROJECT_RELATION_TESTS +
    READ_RELATION_TESTS +
    SET_RELATION_TESTS +
    SORT_RELATION_TESTS +
    WRITE_RELATION_TESTS +
    APPROXIMATION_AGGREGATE_FUNCTIONS +
    ARITHMETIC_AGGREGATE_FUNCTIONS +
    ARITHMETIC_DECIMAL_AGGREGATE_FUNCTIONS +
    BOOLEAN_AGGREGATE_FUNCTIONS +
    STRING_AGGREGATE_FUNCTIONS +
    ARITHMETIC_SCALAR_FUNCTIONS +
    ARITHMETIC_DECIMAL_SCALAR_FUNCTIONS +
    BOOLEAN_SCALAR_FUNCTIONS +
    COMPARISON_SCALAR_FUNCTIONS +
    DATETIME_SCALAR_FUNCTIONS +
    LOGARITHMIC_SCALAR_FUNCTIONS +
    ROUNDING_SCALAR_FUNCTIONS +
    STRING_SCALAR_FUNCTIONS
)

def transform_test(test):
    sql_str, producers = test["sql_query"]
    producers = [p.name() for p in producers]
    sql_lines = sql_str.split("\n")
    sql_lines = [l.strip() for l in sql_lines if l.strip()]
    sql_str = " ".join(sql_lines)
    test["sql_query"] = {
        "query": sql_str,
        "producers": producers
    }
    return test

TESTS = (transform_test(t) for t in TESTS)

for test in TESTS:
    path = CURRENT_DIR / Path(test["class"]) / Path(test["test_name"] + ".json")
    os.makedirs(path.parent, exist_ok=True)
    with open(path, "w") as f:
        test.pop("class")
        json.dump(test, fp=f, indent=4, sort_keys=True)

