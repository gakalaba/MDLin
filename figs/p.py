with open("mean1sh_f1/5ms.csv", "r") as file:

    lines = file.readlines()

    records = [[value for value in line.split(",")] for line in lines]

# having formatted the dates, sort records by first (date) field:
print(records)
sorted_records = sorted(records, key = lambda r: float(r[0]))

# join values with commas once more, removing newline characters
prepared_lines = [",".join(record).strip("\n") for record in sorted_records]

prepared_data = "\n".join(prepared_lines)

# write out the CSV
with open("mean1sh_f1/5ms2.csv", "w") as file:
    file.write(prepared_data)
