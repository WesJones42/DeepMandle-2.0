import os
import pandas as pd
import argparse

def combine_csv_files(file_list, output_file, with_iterations=False):
    if with_iterations:
        combined_df = pd.concat([pd.read_csv(f, sep=' ', header=None) for f in file_list], ignore_index=True)
    else:
        combined_df = pd.concat([pd.read_csv(f, sep=' ', header=None, usecols=[0, 1]) for f in file_list], ignore_index=True)

    combined_df.to_csv(output_file, index=False, header=False, sep=' ')
    print(f"Combined files.")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('folder_path', type=str)
    parser.add_argument('output_file', type=str)
    parser.add_argument('--with-iterations', action='store_true')

    args = parser.parse_args()

    file_list = [os.path.join(args.folder_path, f) for f in os.listdir(args.folder_path) if f.endswith('.csv')]
    combine_csv_files(file_list, args.output_file, args.with_iterations)

if __name__ == "__main__":
    main()

