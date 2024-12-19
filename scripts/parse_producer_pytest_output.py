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

# Remove skipped tests with incomplete information.
df = df[df.outcome.notna()]

# Bring into desired output format.
df["FullFunction"] = df.group + "." + df.name
df.status = df.outcome.astype(str) == "True"

# Pivot such that each producer/consumer pair becomes one columns.
df = df.pivot(index="FullFunction", columns="producer", values="status")

# Count any combinations that didn't exist as "failed".
df = df.fillna("False")

# Write out result.
df.to_csv(args.output, sep=",")
