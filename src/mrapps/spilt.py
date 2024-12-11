import csv
import math

input_file = 'user_matrix.csv'
output_prefix = 'user_split_'
num_splits = 8

# Read all rows from the input CSV
with open(input_file, 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.reader(csvfile)
    rows = list(reader)

total_rows = len(rows)
rows_per_file = math.ceil(total_rows / num_splits)

for i in range(num_splits):
    start_index = i * rows_per_file
    end_index = start_index + rows_per_file
    split_rows = rows[start_index:end_index]
    output_file = f'{output_prefix}{i+1}.txt'
    with open(output_file, 'w', encoding='utf-8') as txtfile:
        for row in split_rows:
            txtfile.write(','.join(row) + '\n')