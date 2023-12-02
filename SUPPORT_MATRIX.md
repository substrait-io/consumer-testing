Substrait Producer and Consumer Function Support
================================================

# Substrait Producer Function Support

[Substrait Producer x Function](https://consumer-testing-crahxzgvruqwmujgry3r2u.streamlit.app/)

[Raw table of test results](https://github.com/richtia/consumer-testing/blob/producer_support_matrix/app/producer_results.csv)

## How to regenerate test results
```commandline
cd substrait_consumer/tests/functional/extension_functions
pytest --csv producer_pytest_output.csv --csv-delimiter ';' --csv-columns 'id,status' -m produce_substrait_snapshot
```


# Substrait Producer x Consumer x Function Support

[Substrait Producer x Substrait Consumer x Function](https://consumer-testing-qomfxmpyspcrpkatq3uz3g.streamlit.app/)

[Raw table of test results](https://github.com/richtia/consumer-testing/blob/producer_support_matrix/app/consumer_results.csv)

## How to regenerate test results
```commandline
cd substrait_consumer/tests/functional/extension_functions
pytest --csv consumer_pytest_output.csv --csv-delimiter ';' --csv-columns 'id,status' -m consume_substrait_snapshot
```

