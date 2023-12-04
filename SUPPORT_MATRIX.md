Substrait Producer and Consumer Function Support
================================================

# Substrait Producer Function Support Matrix

[Substrait Producer x Function](https://consumer-testing-crahxzgvruqwmujgry3r2u.streamlit.app/)

[Raw table of test results](https://github.com/richtia/consumer-testing/blob/producer_support_matrix/app/producer_results.csv)

## How to regenerate producer test results
```commandline
cd substrait_consumer/tests/functional/extension_functions

# Run the producer tests to create the pytest result output.
pytest --csv producer_pytest_output.csv --csv-delimiter ';' --csv-columns 'id,status' -m produce_substrait_snapshot

# Parse pytest output.  This creates consumer-testing/app/producer_results.csv, which
# the streamlit app uses to generate the table.
python parse_producer_pytest_output.py
```


# Substrait Producer x Consumer x Function Support Matrix

[Substrait Producer x Substrait Consumer x Function](https://consumer-testing-qomfxmpyspcrpkatq3uz3g.streamlit.app/)

[Raw table of test results](https://github.com/richtia/consumer-testing/blob/producer_support_matrix/app/consumer_results.csv)

## How to regenerate consumer test results
```commandline
cd substrait_consumer/tests/functional/extension_functions

# Run the consumer tests to create the pytest result output.
pytest --csv consumer_pytest_output.csv --csv-delimiter ';' --csv-columns 'id,status' -m consume_substrait_snapshot

# Parse pytest output.  This creates consumer-testing/app/consumer_results.csv, which
# the streamlit app uses to generate the table.
python parse_consumer_pytest_output.py
```

