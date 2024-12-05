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
  * [Updating Snapshots](#Updating-Snapshots)
    * [Substrait Plan Snapshots](#Substrait-Plan-Snapshots)
    * [Results Snapshots](#Results-Snapshots)
* [CLI Tool](#Generating-Substrait-from-Adhoc-SQL-Ibis)
  * [How to Use](#How-to-Use)
* [How to Add Producers](#How-to-Add-Producers)
* [How to Add Consumers](#How-to-Add-Consumers)


# Overview
This testing repository provides instructions on how to add and run substrait integration 
tests as well as a CLI tool for generating substrait plans from adhoc SQL queries.  

The tests are organized into two categories; tpch tests (which test common benchmark queries) 
and substrait function tests (which test individual extension functions). Test data is created 
using DuckDB at the start of the test class using the `prepare_tpch_parquet_data` fixture, 
which is located in `substrait_consumer/conftest.py`.

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

Install the project in the developer mode (recommended)

```commandline
git submodule init
git submodule update --init
cd consumer-testing
./build-and-copy-isthmus-shadow-jar.sh
```

*This shell script may not work on Windows environments.*

```bash
python3 setup.py develop
```

# How to Run Tests
TPCH tests are located in the `substrait_consumer/tests/integration` folder and substrait function tests
are located in the `substrait_consumer/tests/functional` folder.

Tests are run with pytest.

TPCH Tests:
```commandline
cd substrait_consumer/tests/integration/
pytest test_acero_tpch.py
```

Function Tests:
```commandline
cd substrait_consumer/tests/functional/

# Run all function tests:
pytest extension_functions

# Run a single function test:
pytest extension_functions/test_arithmetic_functions.py
```

# TPCH Tests
TPCH test files are located in the `substrait_consumer/tests/integration` folder.


## Test Case Args
Test case arguments are located in `substrait_consumer/tests/integration/queries/tpch_test_cases.py`.  They specify 
the parquet files, the SQL query, and substrait query plan that will be used for the test cases.

query_1.py
```python
TPCH_QUERY_TESTS = (
    {
        "test_name": "test_tpch_sql_1",
        "local_files": {},
        "named_tables": {"lineitem": "lineitem.parquet"},
        "sql_query": get_sql("q1.sql"),
        "substrait_query": get_substrait_plan("query_01_plan.json"),
    },
    {
        "test_name": "test_tpch_sql_2",
        "local_files": {},
        "named_tables": {
            "part": "part.parquet",
            "supplier": "supplier.parquet",
            "partsupp": "partsupp.parquet",
            "nation": "nation.parquet",
            "region": "region.parquet",
            "partsupp": "partsupp.parquet",
            "supplier": "supplier.parquet",
            "nation": "nation.parquet",
            "region": "region.parquet",
        },
        "sql_query": get_sql("q2.sql"),
        "substrait_query": get_substrait_plan("query_02_plan.json"),
    },
)
```
## Substrait Plans
Substrait query plans are located in `substrait_consumer/tests/integration/queries/tpch_substrait_plans`.

## SQL Queries
SQL queries are located in `substrait_consumer/tests/integration/queries/tpch_sql`.

The SQL queries have named placeholders (`'{customer}'`) where the table names or file paths will be inserted.
Table names are determined based on the `"named_tables"` and `"local_files"` in the test case args file.

# Function Tests
The substrait function tests aim to test the functions available in Substrait.  This is done
by converting queries (SQL/Ibis expressions) into substrait plans via various producers and
running the substrait plans on different consumers.  

The producer tests take the SQL/Ibis query and generate a substrait plan in json format. 
These plans are saved as snapshots, [using pytest-snapshot](https://pypi.org/project/pytest-snapshot/),
to be used later on for verification as well as an input to the consumer tests.

The consumer tests read the saved substrait plan snapshots and generate results. These
results are saved as a snapshot to be used for verification.

If there is a mismatch between results and a saved snapshot, the test will fail and a diff
will be generated.

Substrait function test files are located in the `substrait_consumer/functional/extension_functions` folder.


## Test Case Args
Test case arguments located in `substrait_consumer/functional/queries/{*_tests}.py`.  They specify 
the parquet files, an SQL query, and an ibis expression.

The tests also take in the consumer and producer as test input via the producer/consumer test fixtures,
which are defined in `substrait_consumer/conftest.py`.  The fixtures allow the tests to cycle through all combinations
of producers and consumers.

arithmetic_tests.py
```python
SCALAR_FUNCTIONS = (
    {
        "test_name": "add",
        "local_files": {},
        "named_tables": {"partsupp": "partsupp.parquet"},
        "sql_query": SQL_SCALAR["add"],
        "ibis_expr": IBIS_SCALAR["add"],
    },
```

## SQL Queries
The SQL queries are located in `substrait_consumer/functional/queries/sql`.
arithmetic_functions_sql.py
```python
SQL_SCALAR = {
    "add":
        """
        SELECT PS_PARTKEY, PS_SUPPKEY, add(PS_PARTKEY, PS_SUPPKEY) AS ADD_KEY
        FROM '{partsupp}';
        """,
```

## Ibis Expressions
The Ibis expressions are located in `substrait_consumer/functional/queries/ibis_expressions`.
arithmetic_functions_expr.py
```python
def add_expr(partsupp, lineitem, t):
    new_col = (partsupp.ps_partkey + partsupp.ps_suppkey).name("ADD_KEY")
    return partsupp[partsupp.ps_partkey, partsupp.ps_suppkey, new_col]

IBIS_SCALAR = {
    "add": add_expr,
}
```

## Updating Snapshots

### Substrait Plan Snapshots
Each producer has its own set of Substrait plan snapshots that are stored in
`*_plan.json` files under
`substrait_consumer/tests/functional/extension_functions/snapshots/` and
`substrait_consumer/tests/functional/relations/snapshots/`:
```commandline
$ ls substrait_consumer/tests/functional/extension_functions/snapshots/producer/approximation/
approx_count_distinct-duckdb_plan.json  approx_distinct-datafusion_plan.json
$ ls substrait_consumer/tests/functional/relations/snapshots/producer/fetch/
fetch-isthmus_plan.json  fetch_with_offset-isthmus_plan.json
```

Substrait plan snapshots are used to verify that producers are able to generate substrait plans.
These tests are marked with the `produce_substrait_snapshot` pytest marker.

Snapshots can be updated with the following command:
```commandline
cd substrait_consumer/tests/functional/extension_functions
pytest -m produce_substrait_snapshot --snapshot-update
```
You can update the snapshots from a single producer with the `--producer` option as well as
for a single test by specifying the test file:
```commandline
pytest -m produce_substrait_snapshot --producer isthmus --snapshot-update test_arithmetic_functions.py
```


### Results Snapshots
Results Snapshots are generated by running the SQL query corresponding to the function under
test against DuckDB.  Those tests are marked with the `generate_function_snapshots` pytest
marker.
```commandline
cd substrait_consumer/tests/functional/extension_functions
pytest -m generate_function_snapshots --snapshot-update test_arithmetic_functions.py
```

Results snapshots are saved `*_result.txt` files under
`substrait_consumer/tests/functional/extension_functions/snapshots/` and
`substrait_consumer/tests/functional/relations/snapshots/`:
```commandline
$ ls substrait_consumer/tests/functional/extension_functions/snapshots/consumer/approximation/
approx_count_distinct_result.txt
$ ls substrait_consumer/tests/functional/relations/snapshots/consumer/fetch/
fetch_result.txt  fetch_with_offset_result.txt
```


# Generating Substrait from Adhoc SQL/Ibis
The CLI tool for generating substrait plans from adhoc SQL queries and Ibis expression
is located in the `substrait_consumer/tests/adhoc` directory.  The SQL queries should be 
written using the same TPCH data used in the integration tests.  This tool will generate 
the substrait plans for each supported producer and run that plan against all supported consumers.

## How to Use
If you are testing out an SQL query, copy your SQL query into `substrait_consumer/tests/adhoc/query.sql`
and run the following command (make sure to specify a producer that can convert SQL to Substrait):
```commandline
cd substrait_consumer/tests/adhoc
pytest --adhoc_producer=isthmus test_adhoc_expression.py
```

If you are testing out an Ibis expression, copy your Ibis expression into 
`substrait_consumer/tests/adhoc/ibis_expr.py` and run the following command:
```commandline
cd substrait_consumer/tests/adhoc
pytest --adhoc_producer=ibis test_adhoc_expression.py
```
*Note: If you're using the IbisProducer, make sure you do not edit the function name and arguments
already in line 2 of `ibis_expr.py`.  The test is expecting the specific name and arguments.

You can save the produced substrait plans with the `--saveplan` option.
```commandline
pytest --saveplan True --adhoc_producer=isthmus test_adhoc_expression.py
```
Plans will be saved as {producer_name}_substrait.json
```commandline
ls *.json
IsthmusProducer_substrait.json
```

If you want to run the tests using specific producer/consumer pairs, you can use 
the both the `--adhoc_producer` and `--consumer` options.
```commandline
pytest --adhoc_producer=isthmus --consumer=acero test_adhoc_expression.py
```



# How to Add Producers
Producers should be added to the `substrait_consumer/producers` folder and provide 
methods on how to produce the substrait query plan.  Look at 
`substrait_consumer/producers/duckdb_producer.py` for an example implementation.

In order for the test to use the new producer, the producer class name should also be added
to the PRODUCERS list in `substrait_consumer/conftest.py`.


# How to Add Consumers
Consumers should be added to the `substrait_consumer/consumers` folder and provide 
methods on how to run the substrait query plan against that consumer.  Look at 
`substrait_consumer/producers/duckdb_consumer.py` for an example implementation.

In order for the test to use the new consumer, the consumer class name should also be added
to the CONSUMERS list in `substrait_consumer/conftest.py`.

# How to Update Producers/Consumers/Dependencies
The versions of all dependencies in `requirements.txt` are frozen in order to
make the tests reproducible. Similarly, the version of the Isthmus producer is
frozen via its version of the git submodule at `substrait-java/`. Development
and CI should mainly happen with those versions in order to eliminate diverging
behavior due to version differences.

In order to update a specific producer or consumer or one or several of the
dependencies, install the desired versions using:

```bash
pip install --upgrade package==1.23.4 # upgrade package to specific version
pip install --upgrade package         # upgrade package to latest version
pip install --upgrade -r requirements-unlocked.txt # upgrade all dependencies to latest version
```

For dependencies in git submodules, update the corresponding submodules.

After the dependency was updated, run the tests and make sure that all changes
in test outcomes are expected and fix potential problems and/or update the
snapshots.
