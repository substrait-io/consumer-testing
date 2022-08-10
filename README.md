Substrait Consumer Integration Tests
====================================

Table of Contents
=================
* [Overview](#Overview)
* [Setup](#Setup)
* [How to Run Tests](#How-to-Run-Tests)
* [Test Files](#Test-Files)
  * [Test Case Args](#Test-Case-Args)
  * [Substrait Plans](#Substrait-Plans)
  * [SQL Queries](#SQL-Queries)
* [How to Add New Consumers](#How-to-Add-New-Consumers)

# Overview
This testing repository provides instructions on how to add and run substrait consumer 
integration tests.  These tests take in parquet files, a SQL query, and a json-formatted substrait 
query plan as input.  The tests will run the SQL query on DuckDB, and the substrait plan on the 
specified consumer, and then compare the two results. The test data is created using DuckDB at the 
start of the test class using the `prepare_tpch_parquet_data` fixture, which is located in 
`tests/conftest.py`.

# Setup
Create and activate your conda environment with python3.9:
```commandline
conda create -y -n substrait_consumer_testing -c conda-forge python=3.9
conda activate substrait_consumer_testing
```

Install requirements from the top level directory:
```commandline
pip install -r requirements.txt
```
# How to Run Tests
Tests are located in the `tests/integration` folder and are run with pytest.
```commandline
pytest test_acero_tpch.py
```

# Test Files
Testing methods are located in the `tests/integration` folder and prefixed with `test_`. 
For example, `test_acero_tpch.py`


## Test Cases
Test case arguments located in `tests/integration/queries/tpch_test_cases.py`.  They specify 
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
 

# How to Add New Consumers
Consumers should be added to the `tests/consumers` directory and provide 
methods on how to run the substrait query plan against that consumer.  Look at 
`tests/consumers/acero_consumer.py` for an example implementation.
