import pandas as pd

def split_csv_by_quarter(input_file, output_prefix='2024_Q'):
    
    # Read the main CSV file
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Display basic info
    print(f"Total rows: {len(df)}")
    print(f"Date range: {df['fl_date'].min()} to {df['fl_date'].max()}")
    
    # Convert fl_date to datetime if it's not already
    df['fl_date'] = pd.to_datetime(df['fl_date'])
    
    # Define quarter date ranges for 2024
    quarters = {
        'Q1': ('2024-01-01', '2024-03-31'),
        'Q2': ('2024-04-01', '2024-06-30'),
        'Q3': ('2024-07-01', '2024-09-30'),
        'Q4': ('2024-10-01', '2024-12-31')
    }
    
    # Split and save each quarter
    for quarter, (start_date, end_date) in quarters.items():
        # Filter data for the quarter
        mask = (df['fl_date'] >= start_date) & (df['fl_date'] <= end_date)
        quarter_df = df[mask]
        
        # Create output filename
        output_file = f"{output_prefix}{quarter}.csv"
        
        # Save to CSV
        quarter_df.to_csv(output_file, index=False)
        
        print(f"{quarter}: {len(quarter_df)} rows -> {output_file}")
    
    print("\nSplit complete!")

# Run the function with your file
split_csv_by_quarter('flight_data_2024.csv')