import csv
import re
import sys

try:
    PARSED_RESULTS_FILE = sys.argv[1]
except IndexError:
    print("Please specify an output file")
    sys.exit(1)
DATA_DICT = {}

with open("./producer_pytest_output.csv", "r") as file:
    csvreader = csv.reader(file, delimiter=";")
    rowcount = 0
    for row in csvreader:
        print(row)
        if rowcount > 0:
            data = row[0].split("::")[-1]
            status = row[1]
            function_group = None
            try:
                function_group = re.search(r"producer_(.*?)_functions\[", data).group(1)
            except AttributeError:
                print(f"No producer function found in test case: {data}")
            producer, function = re.search(r"\[(.*?)\]", data).group(1).split("-")
            full_operation = function_group + "." + function
            if status == "passed":
                status = "True"
            else:
                status = "False"
            if full_operation not in DATA_DICT:
                DATA_DICT[full_operation] = [(producer, status)]
            else:
                DATA_DICT[full_operation].append((producer, status))
        rowcount += 1

with open(PARSED_RESULTS_FILE, "w", newline="") as csvfile:
    writer = csv.writer(
        csvfile, escapechar=" ", quoting=csv.QUOTE_NONE, quotechar=" ", delimiter=" "
    )
    writer.writerow(
        ["FullFunction,DataFusionProducer,DuckDBProducer,IbisProducer,IsthmusProducer"]
    )
    rowcount = 0
    for key, value in DATA_DICT.items():
        sorted_by_prod = sorted(value, key=lambda x: x[0])
        DATA_DICT[key] = sorted_by_prod
        statuses_list = [key]
        for pair in sorted_by_prod:
            statuses_list.append(pair[1])
        writer.writerow([",".join(statuses_list)])
