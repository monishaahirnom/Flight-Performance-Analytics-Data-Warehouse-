"""
================================================================================
CSV IMPORTER - FIXED VERSION (NAType issue resolved)
================================================================================
"""

import pandas as pd
import numpy as np
import pyodbc
import logging
from datetime import datetime

# Configuration
SERVER = 'JILL\\SQLEXPRESS'
DATABASE = 'flight_analytics'
CSV_FOLDER = r'C:\Users\patel\OneDrive - SARDAR VALLABHBHAI PATEL INSTITUTE OF TECHNOLOGY, SVIT\Desktop\UWin\Semester 2\ADT\Project\Project\datasets'

CSV_FILES = {
    'Q1': '2024_Q1.csv',
    'Q2': '2024_Q2.csv',
    'Q3': '2024_Q3.csv',
    'Q4': '2024_Q4.csv'
}

BATCH_SIZE = 10000

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('csv_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_connection():
    conn_str = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;'
    return pyodbc.connect(conn_str, timeout=60)

def clean_data_for_sql(df):
    """Convert pandas NA types to Python None"""
    logger.info("Converting data types for SQL Server compatibility...")

    # Replace pd.NA and np.nan with None
    df = df.replace({pd.NA: None, np.nan: None})

    # Convert Int64 (nullable int) to regular int or None
    int_cols = ['crs_dep_time', 'dep_time', 'crs_arr_time', 'arr_time', 'cancelled', 'diverted']
    for col in int_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: int(x) if pd.notna(x) and x is not None and x is not pd.NA else None)

    # Ensure strings are proper str or None
    str_cols = ['op_unique_carrier', 'op_carrier_fl_num', 'origin', 'dest', 'cancellation_code']
    for col in str_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and x is not None and x is not pd.NA else None)

    # Convert floats properly
    float_cols = ['dep_delay', 'arr_delay', 'taxi_out', 'taxi_in', 'crs_elapsed_time', 
                  'actual_elapsed_time', 'air_time', 'distance', 'carrier_delay', 
                  'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay']
    for col in float_cols:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: float(x) if pd.notna(x) and x is not None and x is not pd.NA else None)

    return df

def import_csv_to_table(csv_path, table_name):
    """Import CSV with CORRECT data types"""
    logger.info(f"="*80)
    logger.info(f"Importing {csv_path} into {table_name}")
    logger.info(f"="*80)

    start_time = datetime.now()

    # Read CSV
    logger.info("Reading CSV file...")

    # Read only the 25 columns we need
    usecols = [
        'fl_date', 'op_unique_carrier', 'op_carrier_fl_num',
        'origin', 'dest',
        'crs_dep_time', 'dep_time', 'crs_arr_time', 'arr_time',
        'dep_delay', 'arr_delay',
        'taxi_out', 'taxi_in',
        'crs_elapsed_time', 'actual_elapsed_time', 'air_time',
        'distance',
        'cancelled', 'cancellation_code', 'diverted',
        'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay'
    ]

    # Read with minimal dtype specification
    df = pd.read_csv(
        csv_path,
        usecols=usecols,
        parse_dates=['fl_date'],
        low_memory=False
    )

    total_rows = len(df)
    logger.info(f"Loaded {total_rows:,} rows from CSV")

    # Force op_unique_carrier to string
    df['op_unique_carrier'] = df['op_unique_carrier'].astype(str)
    df.loc[df['op_unique_carrier'] == 'nan', 'op_unique_carrier'] = None

    # Show sample carriers
    unique_carriers = df['op_unique_carrier'].unique()
    logger.info(f"Unique carriers found: {sorted([c for c in unique_carriers if c is not None and c != 'None'])}")

    # Clean data for SQL
    df = clean_data_for_sql(df)

    # Connect and insert
    conn = get_connection()
    cursor = conn.cursor()

    # Prepare insert statement
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['?' for _ in df.columns])
    insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    logger.info(f"Starting bulk insert in batches of {BATCH_SIZE:,}...")

    inserted = 0
    for i in range(0, total_rows, BATCH_SIZE):
        batch = df.iloc[i:i+BATCH_SIZE]

        # Convert to list of tuples with proper None handling
        batch_data = []
        for _, row in batch.iterrows():
            row_tuple = tuple(None if pd.isna(val) or val is pd.NA or val is np.nan else val for val in row)
            batch_data.append(row_tuple)

        try:
            cursor.executemany(insert_sql, batch_data)
            conn.commit()
            inserted += len(batch_data)

            if inserted % 50000 == 0 or inserted == total_rows:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = inserted / elapsed
                remaining = (total_rows - inserted) / rate if rate > 0 else 0
                logger.info(f"Progress: {inserted:,}/{total_rows:,} ({inserted/total_rows*100:.1f}%) - ETA: {remaining/60:.1f} min")

        except Exception as e:
            logger.error(f"Batch failed at row {i}: {e}")
            conn.rollback()
            cursor.close()
            conn.close()
            raise

    cursor.close()
    conn.close()

    duration = (datetime.now() - start_time).total_seconds()
    logger.info(f"✓ {table_name} completed: {inserted:,} rows in {duration/60:.1f} minutes")
    logger.info("")

def main():
    logger.info("="*80)
    logger.info("CSV IMPORT SCRIPT STARTED (FIXED VERSION)")
    logger.info(f"Time: {datetime.now()}")
    logger.info("="*80)
    logger.info("")

    overall_start = datetime.now()

    try:
        for table, filename in CSV_FILES.items():
            csv_path = f"{CSV_FOLDER}\\{filename}"
            import_csv_to_table(csv_path, table)

        logger.info("="*80)
        logger.info("ALL IMPORTS COMPLETED SUCCESSFULLY!")
        logger.info("="*80)

        # Verify results
        logger.info("\nVerifying imports...")
        conn = get_connection()
        cursor = conn.cursor()

        for table in CSV_FILES.keys():
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            logger.info(f"  {table}: {count:,} records")

        logger.info("\nChecking carrier codes...")
        cursor.execute("""
            SELECT DISTINCT op_unique_carrier 
            FROM Q1
            UNION SELECT DISTINCT op_unique_carrier FROM Q2
            UNION SELECT DISTINCT op_unique_carrier FROM Q3
            UNION SELECT DISTINCT op_unique_carrier FROM Q4
            ORDER BY op_unique_carrier
        """)
        carriers = [row[0] for row in cursor.fetchall() if row[0] is not None]
        logger.info(f"  Unique carriers: {carriers}")
        logger.info(f"  Total carriers: {len(carriers)}")

        cursor.close()
        conn.close()

        total_time = (datetime.now() - overall_start).total_seconds()
        logger.info(f"\nTotal execution time: {total_time/60:.1f} minutes")
        logger.info("="*80)
        logger.info("✓ SUCCESS - Ready for ETL pipeline!")
        logger.info("="*80)

    except Exception as e:
        logger.error(f"\n{'='*80}")
        logger.error(f"IMPORT FAILED: {e}")
        logger.error(f"{'='*80}")
        raise

if __name__ == "__main__":
    main()
