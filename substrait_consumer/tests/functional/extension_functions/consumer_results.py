import csv
import re

data_dict = {}

with open("./consumer_pytest_output.csv", 'r') as file:
    csvreader = csv.reader(file, delimiter=';')
    rowcount = 0
    for row in csvreader:
        print(row)
        if rowcount > 0:
            data = row[0].split("::")[-1]
            status = row[1]
            function_group = re.search(r'consumer_(.*?)_functions\[', data).group(1)
            producer, consumer, function = re.search(r'\[(.*?)\]', data).group(1).split('-')
            full_operation = function_group + "." + function
            if status == "passed":
                status = "True"
            else:
                status = "False"
            if full_operation not in data_dict:
                data_dict[full_operation] = [(producer, consumer, status)]
            else:
                data_dict[full_operation].append((producer, consumer, status))
        rowcount += 1

with open('../../../../app/consumer_results.csv', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile, escapechar=' ', quoting=csv.QUOTE_NONE, quotechar=' ', delimiter=' ')
    header = False
    rowcount = 0
    for key, value in data_dict.items():
        sorted_by_producer = sorted(value, key=lambda x: x[0])
        if not header:
            firstrow = "FullFunction"
            for tup in sorted_by_producer:
                firstrow += "," + f"{tup[0]}-{tup[1]}"
            writer.writerow([firstrow])
            header = True
        data_dict[key] = sorted_by_producer
        statuses_list = [key]
        for producer_consumer_status in sorted_by_producer:
            statuses_list.append(producer_consumer_status[2])
        writer.writerow([','.join(statuses_list)])
