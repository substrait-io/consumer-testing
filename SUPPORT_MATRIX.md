Substrait Producer and Consumer Function Support
================================================

# Substrait Producer Function Support Matrix

[Substrait Producer x Function](https://substrait-function-compatibility.streamlit.app/)

[Raw table of test results](https://raw.githubusercontent.com/substrait-io/consumer-testing/main/app/producer_results.csv)

## How to regenerate producer test results
```commandline
cd substrait_consumer/tests/functional/extension_functions

# Run the producer tests to create the pytest result output.
pytest --csv producer_pytest_output.csv --csv-delimiter ';' --csv-columns 'id,status' -m produce_substrait_snapshot

# Parse pytest output.  This creates a new csv file with the parsed results, which
# the streamlit app uses to generate the table.
python parse_producer_pytest_output.py producer_results.csv
```


# Substrait Producer x Consumer x Function Support Matrix

[Substrait Producer x Substrait Consumer x Function](https://substrait-consumer-compatibility.streamlit.app/)

[Raw table of test results](https://raw.githubusercontent.com/substrait-io/consumer-testing/main/app/consumer_results.csv)

## How to regenerate consumer test results
```commandline
cd substrait_consumer/tests/functional/extension_functions

# Run the consumer tests to create the pytest result output.
pytest --csv consumer_pytest_output.csv --csv-delimiter ';' --csv-columns 'id,status' -m consume_substrait_snapshot

# Parse pytest output.  This creates a new csv file with the parsed results, which
# the streamlit app uses to generate the table.
python parse_consumer_pytest_output.py consumer_results.csv
```

