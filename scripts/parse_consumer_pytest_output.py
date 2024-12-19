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

# Compute full function name.
df["FullFunction"] = df.group + "." + df.name

# Remove skipped tests with incomplete information.
df = df[df.FullFunction.notna()]

# Make sure we have all combinations of (producer, consumer, FullFunction).
producers = pd.DataFrame(df.producer.unique())
consumers = pd.DataFrame(df.consumer.unique())
functions = pd.DataFrame(df.FullFunction.unique())
df_all = pd.merge(producers, consumers, how="cross")
df_all = pd.merge(df_all, functions, how="cross")
df_all.columns = ["producer", "consumer", "FullFunction"]
df = pd.merge(df_all, df, how="left")

# Compute desired output columns.
df["producer-consumer"] = df.producer + "-" + df.consumer
df.status = df.data_outcome.astype(str) == "True"
df = df.fillna("False")

# Pivot such that each producer/consumer pair becomes one columns.
df = df.pivot(index="FullFunction", columns="producer-consumer", values="status")

# Count any combinations that didn't exist as "failed".

# Write out result.
df.to_csv(args.output, sep=",")
