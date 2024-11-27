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
regex = r".*producer_(.+)_functions\[(.+)-(.+)\]"
df["function_group"] = df.test_name.str.replace(regex, lambda m: m.group(1), regex=True)
df["producer"] = df.test_name.str.replace(regex, lambda m: m.group(2), regex=True)
df["function"] = df.test_name.str.replace(regex, lambda m: m.group(3), regex=True)

# Bring into desired output format.
df["FullFunction"] = df.function_group + "." + df.function
df.status = df.outcome == "True"

# Pivot such that each producer/consumer pair becomes one columns.
df = df.pivot(index="FullFunction", columns="producer", values="status")

# Write out result.
df.to_csv(args.output, sep=",")
