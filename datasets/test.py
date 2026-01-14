import pandas as pd

# Configuration
files = {
    'Main file': 'flight_data_2024.csv',
    'Quarter 1': '2024_QQ1.csv',
    'Quarter 2': '2024_QQ2.csv',
    'Quarter 3': '2024_QQ3.csv',
    'Quarter 4': '2024_QQ4.csv'
}

# Function to get file stats without loading entire file
def get_file_stats(filepath):
    # Read only the fl_date column to minimize memory
    date_col = pd.read_csv(filepath, usecols=['fl_date'])
    record_count = len(date_col)
    min_date = date_col['fl_date'].min()
    max_date = date_col['fl_date'].max()
    return record_count, min_date, max_date

# Collect stats for all files
stats = {}
for name, filepath in files.items():
    count, min_date, max_date = get_file_stats(filepath)
    stats[name] = count
    print(f"Total records in {name}: {count:,}")
    print(f"Date range: {min_date} to {max_date}\n")

# Verify sum of quarters matches main file
quarter_sum = sum(stats[f'Quarter {i}'] for i in range(1, 5))
if stats['Main file'] == quarter_sum:
    print("Test passed: The sum of records in all quarters matches the main file.")
else:
    print("Test failed: The sum of records in all quarters does not match the main file.")