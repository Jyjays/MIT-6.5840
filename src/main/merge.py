import os
import argparse

def merge_files(output_file, input_directory):
    with open(output_file, 'w') as outfile:
        for filename in os.listdir(input_directory):
            if filename.startswith('mr-out-'):
                with open(os.path.join(input_directory, filename), 'r') as infile:
                    for line in infile:
                        outfile.write(line)

def split_file(input_file, prefix, num_parts=8):
    with open(input_file, 'r') as f:
        lines = f.readlines()
        total = len(lines)
        part_size = total // num_parts
        for i in range(num_parts):
            start = i * part_size
            end = start + part_size if i < num_parts - 1 else total
            with open(f'input_files/{prefix}_{i+1}.txt', 'w') as outfile:
                outfile.writelines(lines[start:end])

def main():
    parser = argparse.ArgumentParser(description='Merge and optionally split files.')
    parser.add_argument('-n', '--name', required=True, help='Name of the merged output file')
    parser.add_argument('-s', '--split', nargs='?', const='split', help='Split the merged file with the given prefix name')
    args = parser.parse_args()

    output_file = args.name
    input_directory = '/home/jyjays/MIT6.5840/6.5840/src/main/'

    merge_files(output_file, input_directory)

    if args.split:
        split_prefix = args.split
        split_file(output_file, split_prefix)

if __name__ == "__main__":
    main()