import csv

def is_float(value):
    try:
        float(value)
        return True
    except ValueError:
        return False

def count_zeroes(filename):
    total_zero_pairs = 0
    total_single_zeros = 0
    total_points = 0

    with open(filename, 'r') as file:
        reader = csv.reader(file, delimiter=' ')

        for row in reader:
            total_points += 1

            iterations = row[2:]  

            for i in range(len(iterations)):
                if is_float(iterations[i]):
                    current_value = float(iterations[i])

                    if current_value == 0.0:
                        total_single_zeros += 1

                    if i < len(iterations) - 1 and is_float(iterations[i + 1]):
                        next_value = float(iterations[i + 1])
                        if current_value == 0.0 and next_value == 0.0:
                            total_zero_pairs += 1
                            i += 1  

    return total_zero_pairs, total_single_zeros, total_points

filename = './csv_iterations_d80/combined_iterations.csv'
zero_pair_count, single_zero_count, total_initial_points = count_zeroes(filename)
print(f"Total number of (0,0) iterations: {zero_pair_count}")
print(f"Total number of single 0 iterations: {single_zero_count}")
print(f"Total number of initial points: {total_initial_points}")

