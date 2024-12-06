import argparse

import pandas as pd

# Set up argument parsing.
parser = argparse.ArgumentParser(prog="consumer pytest output parser")
parser.add_argument(
    "-i", "--input", help="Path to the input CSV file created by pytest"
)
parser.add_argument("-o", "--output", help="Path to the output CSV file")
args = parser.parse_args()

# Read CSV file.
df = pd.read_csv(args.input, sep=";")

# Decompose test name into various components
df["test_name"] = df.id.str.split("::").str[-1]
regex = r".*consumer_(.+)_functions\[(.+)-(.+)-(.+)\]"
df["function_group"] = df.test_name.str.replace(regex, lambda m: m.group(1), regex=True)
df["producer"] = df.test_name.str.replace(regex, lambda m: m.group(2), regex=True)
df["consumer"] = df.test_name.str.replace(regex, lambda m: m.group(3), regex=True)
df["function"] = df.test_name.str.replace(regex, lambda m: m.group(4), regex=True)

# Bring into desired output format.
df["FullFunction"] = df.function_group + "." + df.function
df["producer-consumer"] = df.producer + "-" + df.consumer
df.status = df.status == "passed"

# Pivot such that each producer/consumer pair becomes one columns.
df = df.pivot(index="FullFunction", columns="producer-consumer", values="status")

# Temporarily use the old column order to make CI happy.
df = df[
    [
        "datafusion-acero",
        "datafusion-datafusion",
        "datafusion-duckdb",
        "duckdb-acero",
        "duckdb-datafusion",
        "duckdb-duckdb",
        "ibis-datafusion",
        "ibis-acero",
        "ibis-duckdb",
        "isthmus-duckdb",
        "isthmus-datafusion",
        "isthmus-acero",
    ]
]

# Write out result.
df.to_csv(args.output, sep=",")
