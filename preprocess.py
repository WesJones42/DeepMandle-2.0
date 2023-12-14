import csv
from tqdm import tqdm

def process_string(s):
    parts = s.split()
    processed_parts = [part.replace('unstable', '0').replace('stable', '1') for part in parts]
    return ' '.join(processed_parts)

with open('small_iterations/combined_iterations.csv', mode='r') as infile, open('small_processed_fixed.csv', mode='w', newline='') as outfile:
    reader = csv.reader(infile, delimiter=' ')
    writer = csv.writer(outfile, delimiter=' ', quoting=csv.QUOTE_MINIMAL)

    for row in tqdm(reader, desc="Processing rows"):
        quoted_string = ' '.join(row).replace('"', '')
        processed_string = process_string(quoted_string)

        writer.writerow([processed_string])

print("Processing complete.")

