import csv
import re

producer_dict = {}
data_dict = {}

with open("./producer_pytest_output.csv", "r") as file:
    csvreader = csv.reader(file, delimiter=";")
    rowcount = 0
    for row in csvreader:
        print(row)
        if rowcount > 0:
            data = row[0].split("::")[-1]
            status = row[1]
            function_group = re.search(r"producer_(.*?)_functions\[", data).group(1)
            producer, function = re.search(r"\[(.*?)\]", data).group(1).split("-")
            full_operation = function_group + "." + function
            if status == "passed":
                status = "True"
            else:
                status = "False"
            if full_operation not in data_dict:
                data_dict[full_operation] = [(producer, status)]
            else:
                data_dict[full_operation].append((producer, status))
        rowcount += 1

with open("../../../../app/producer_results.csv", "w", newline="") as csvfile:
    writer = csv.writer(
        csvfile, escapechar=" ", quoting=csv.QUOTE_NONE, quotechar=" ", delimiter=" "
    )
    writer.writerow(
        ["FullFunction,DataFusionProducer,DuckDBProducer,IbisProducer,IsthmusProducer"]
    )
    rowcount = 0
    for key, value in data_dict.items():
        sorted_by_prod = sorted(value, key=lambda x: x[0])
        data_dict[key] = sorted_by_prod
        statuses_list = [key]
        for pair in sorted_by_prod:
            statuses_list.append(pair[1])
        writer.writerow([",".join(statuses_list)])
