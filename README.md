Substrait Consumer Integration Tests
====================================

Table of Contents
=================
* [Overview](#Overview)
* [Setup](#Setup)
* [How to Run Tests](#How-to-Run-Tests)
* [TPCH Tests](#TPCH-Tests)
  * [Test Case Args](#Test-Case-Args)
  * [Substrait Plans](#Substrait-Plans)
  * [SQL Queries](#SQL-Queries)
* [Function Tests](#Substrait-Function-Tests)
  * [Test Case Args](#Test-Case-Args)
  * [SQL Queries](#SQL-Queries)
  * [Ibis Expressions](#Ibis-Expressions)
* [How to Add Producers](#How-to-Add-Producers)
* [How to Add Consumers](#How-to-Add-Consumers)


# Overview
This testing repository provides instructions on how to add and run substrait integration 
tests.  The tests are organized into two categories; tpch tests (which test common benchmark queries) 
and substrait function tests (which test individual extension functions). Test data is created 
using DuckDB at the start of the test class using the `prepare_tpch_parquet_data` fixture, 
which is located in `tests/conftest.py`.

# Setup
Create and activate your conda environment with python3.9:
```commandline
conda create -y -n substrait_consumer_testing -c conda-forge python=3.9 openjdk
conda activate substrait_consumer_testing
```
*Note: Java is used by Jpype to access the Isthmus producer.  
JPype should work with all versions of Java but to see details on which versions are 
officially supported see https://jpype.readthedocs.io/en/latest/install.html*

Install requirements from the top level directory:
```commandline
pip install -r requirements.txt
```

Get the java dependencies needed by the Isthmus Substrait producer:
1. Clone the substrait-java repo
2. From the consumer-testing repo, run the build-and-copy-isthmus-shadow-jar.sh script
```commandline
git clone https://github.com/substrait-io/substrait-java.git
cd consumer-testing
sh build-and-copy-isthmus-shadow-jar.sh
```
*This shell script may not work on Windows environments.*

# How to Run Tests
TPCH tests are located in the `tests/integration` folder and substrait function tests
are located in the `tests/functional` folder.

Tests are run with pytest.

TPCH Tests:
```commandline
cd tests/integration/
pytest test_acero_tpch.py
```

Function Tests:
```commandline
cd tests/functional/

Run all function tests:
pytest extension_functions

Run a single function test:
pytest extension_functions/test_arithmetic_functions.py
```

# TPCH Tests
TPCH test files are located in the `tests/integration` folder.


## Test Case Args
Test case arguments are located in `tests/integration/queries/tpch_test_cases.py`.  They specify 
the parquet files, the SQL query, and substrait query plan that will be used for the test cases.

query_1.py
```python
TPCH_QUERY_TESTS = (
    {
        "test_name": "test_tpch_sql_1",
        "file_names": ["lineitem.parquet"],
        "sql_query": get_sql("q1.sql"),
        "substrait_query": get_substrait_plan("query_1_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_2",
        "file_names": [
            "part.parquet",
            "supplier.parquet",
            "partsupp.parquet",
            "nation.parquet",
            "region.parquet",
            "partsupp.parquet",
            "supplier.parquet",
            "nation.parquet",
            "region.parquet",
        ],
        "sql_query": get_sql("q2.sql"),
        "substrait_query": get_substrait_plan("query_2_plan.json"),
    },
]
```
## Substrait Plans
Substrait query plans are located in `tests/integration/queries/tpch_substrait_plans`.
The substrait query plans have placeholder strings in the `local_files` objects in the json 
structure.  
```json
"local_files": {
  "items": [
    {
      "uri_file": "file://FILENAME_PLACEHOLDER_0",
      "parquet": {}
    }
  ]
}
```


When the tests are run, these placeholders are replaced by the parquet data listed 
listed in `"file_names"` in the test case args file. The order of parquet file appearance in the 
`"file_names"` list should be consistent with the ordering for the table names in the substrait 
query plan.

## SQL Queries
SQL queries are located in `tests/integration/queries/tpch_sql`.

The SQL queries have empty bracket placeholders (`'{}'`) where the table names will be inserted. 
Table names are determined based on the `"file_names"` in the test case args file. The order of 
parquet file appearance in the `"file_names"` list should be consistent with the ordering for the 
table names in the SQL query. The actual format after replacement will depend on the consumer being 
used.
 

# Function Tests
The substrait function tests aim to test the functions available in Substrait.  This is done
by converting queries (SQL/Ibis expressions) into substrait plans via various producers and
running the substrait plans on different consumers.  The results of running the substrait plans
are then compared to an expected result, which is determined by running the original query
against a trusted engine (currently we use DuckDB).

Substrait function test files are located in the `tests/functional/extension_functions` folder.


## Test Case Args
Test case arguments located in `tests/functional/queries/{*_tests}.py`.  They specify 
the parquet files, an SQL query, and an ibis expression.

The tests also take in the consumer and producer as test input via the producer/consumer test fixtures,
which are defined in `test/conftest.py`.  The fixtures allow the tests to cycle through all combinations
of producers and consumers.

arithmetic_tests.py
```python
SCALAR_FUNCTIONS = (
    {
        "test_name": "add",
        "file_names": ["partsupp.parquet"],
        "sql_query": SQL_SCALAR["add"],
        "ibis_expr": IBIS_SCALAR["add"],
    },
```

## SQL Queries
The SQL queries are located in `tests/functional/queries/sql`.
arithmetic_functions_sql.py
```python
SQL_SCALAR = {
    "add":
        """
        SELECT PS_PARTKEY, PS_SUPPKEY, add(PS_PARTKEY, PS_SUPPKEY) AS ADD_KEY
        FROM '{}';
        """,
```

## Ibis Expressions
The Ibis expressions are located in `tests/functional/queries/ibis_expressions`.
arithmetic_functions_expr.py
```python
def add_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey + partsupp.ps_suppkey).name("ADD_KEY")
    return partsupp[partsupp.ps_partkey, partsupp.ps_suppkey, new_col]

IBIS_SCALAR = {
    "add": add_expr,
}
```

# How to Add Producers
Producers should be added to the `tests/producers.py` file and provide 
methods on how to produce the substrait query plan.  Look at 
`DuckDBProducer` class for an example implementation.

In order for the test to use the new producer, the producer class name should also be added
to the PRODUCERS list in `tests/conftest.py`.


# How to Add Consumers
Consumers should be added to the `tests/consumers.py` file and provide 
methods on how to run the substrait query plan against that consumer.  Look at 
`AceroConsumer` class for an example implementation.

In order for the test to use the new consumer, the consumer class name should also be added
to the CONSUMERS list in `tests/conftest.py`.
