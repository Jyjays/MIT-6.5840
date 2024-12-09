import os

output_file = '/home/jyjays/MIT6.5840/6.5840/src/main/user_matrix.txt'
input_directory = '/home/jyjays/MIT6.5840/6.5840/src/main/'

with open(output_file, 'w') as outfile:
    for filename in os.listdir(input_directory):
        if filename.startswith('mr-out-'):
            with open(os.path.join(input_directory, filename), 'r') as infile:
                for line in infile:
                    outfile.write(line)
