import pyodbc
import pandas as pd
import numpy as np
from datetime import datetime
import logging
import sys

# ============================================================
# CONFIGURATION
# ============================================================

SERVER = 'JILL\\SQLEXPRESS'
SOURCE_DATABASE = 'flight_analytics'
TARGET_DATABASE = 'FlightDataWarehouse'

SOURCE_CONN_STR = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={SOURCE_DATABASE};Trusted_Connection=yes;'
TARGET_CONN_STR = f'DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={TARGET_DATABASE};Trusted_Connection=yes;'

BATCH_SIZE = 25000
MIN_CLEAN_DATA_PERCENTAGE = 70.0

# Airline code to name mapping (15 airlines)
AIRLINE_NAMES = {
    '9E': 'Endeavor Air',
    'AA': 'American Airlines',
    'AS': 'Alaska Airlines',
    'B6': 'JetBlue Airways',
    'DL': 'Delta Air Lines',
    'F9': 'Frontier Airlines',
    'G4': 'Allegiant Air',
    'HA': 'Hawaiian Airlines',
    'MQ': 'Envoy Air',
    'NK': 'Spirit Airlines',
    'OH': 'PSA Airlines',
    'OO': 'SkyWest Airlines',
    'UA': 'United Airlines',
    'WN': 'Southwest Airlines',
    'YX': 'Republic Airways'
}

# 25 columns available after Step 2 optimization
SELECT_COLUMNS = [
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

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# UTILITY FUNCTIONS
# ============================================================

def get_db_connection(connection_string):
    try:
        conn = pyodbc.connect(connection_string, timeout=30)
        logger.info("Database connection established")
        return conn
    except Exception as e:
        logger.error(f"Connection failed: {e}")
        raise

def clean_dataframe_for_insert(df):
    """Clean dataframe for SQL Server - handle NULLs and invalid floats"""
    df_clean = df.copy()

    # Replace infinity and NaN with None
    df_clean = df_clean.replace([np.inf, -np.inf, np.nan], None)

    # Handle NULLs
    df_clean = df_clean.where(pd.notnull(df_clean), None)

    for col in df_clean.columns:
        if df_clean[col].dtype == 'object':
            df_clean[col] = df_clean[col].apply(
                lambda x: None if (x == '' or (isinstance(x, str) and x.strip() == '')) else x
            )
        elif df_clean[col].dtype in ['float64', 'float32']:
            # Extra check for floats - replace any remaining invalid values
            df_clean[col] = df_clean[col].apply(
                lambda x: None if pd.isna(x) or np.isinf(x) else x
            )

    return df_clean

def bulk_insert(conn, table_name, dataframe, batch_size=BATCH_SIZE):
    """Bulk insert with NUCLEAR float conversion"""
    total_rows = len(dataframe)
    logger.info(f"Starting bulk insert: {total_rows:,} rows into {table_name}")
    
    df_clean = clean_dataframe_for_insert(dataframe)
    cursor = conn.cursor()
    
    columns = ','.join(df_clean.columns)
    placeholders = ','.join(['?' for _ in df_clean.columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    inserted_count = 0
    for i in range(0, total_rows, batch_size):
        batch = df_clean.iloc[i:i+batch_size]
        
        # NUCLEAR: Convert each row explicitly
        batch_data = []
        for _, row in batch.iterrows():
            row_list = []
            for val in row:
                if val is None or pd.isna(val):
                    row_list.append(None)
                elif isinstance(val, (np.integer, np.int64, np.int32)):
                    row_list.append(int(val))
                elif isinstance(val, (np.floating, np.float64, np.float32)):
                    if np.isnan(val) or np.isinf(val):
                        row_list.append(None)
                    else:
                        row_list.append(float(val))
                else:
                    row_list.append(val)
            batch_data.append(tuple(row_list))
        
        retry_count = 0
        max_retries = 3
        
        while retry_count < max_retries:
            try:
                cursor.executemany(insert_query, batch_data)
                conn.commit()
                inserted_count += len(batch_data)
                
                if inserted_count % 50000 == 0 or inserted_count == total_rows:
                    logger.info(f"Progress: {inserted_count:,}/{total_rows:,} rows ({inserted_count/total_rows*100:.1f}%)")
                break
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    logger.warning(f"Batch failed, retry {retry_count}/{max_retries}: {e}")
                else:
                    logger.error(f"Batch failed after {max_retries} retries at row {i}: {e}")
                    cursor.close()
                    raise
    
    cursor.close()
    logger.info(f"Bulk insert completed: {inserted_count:,} rows into {table_name}")

# ============================================================
# DATA QUALITY FUNCTIONS
# ============================================================

def validate_record(row):
    """
    Validate single record - 100% validation
    Mandatory: fl_date, origin, dest, carrier, dep_time, arr_time
    Returns: (is_valid, rejection_reason)
    """
    reasons = []

    # Check mandatory fields
    if pd.isna(row.get('fl_date')) or row.get('fl_date') is None:
        reasons.append('NULL fl_date')

    origin_val = row.get('origin')
    if pd.isna(origin_val) or origin_val is None or str(origin_val).strip() == '':
        reasons.append('NULL origin')

    dest_val = row.get('dest')
    if pd.isna(dest_val) or dest_val is None or str(dest_val).strip() == '':
        reasons.append('NULL dest')

    carrier_val = row.get('op_unique_carrier')
    if pd.isna(carrier_val) or carrier_val is None or str(carrier_val).strip() == '':
        reasons.append('NULL carrier')

    if pd.isna(row.get('dep_time')) or row.get('dep_time') is None:
        reasons.append('NULL dep_time')

    if pd.isna(row.get('arr_time')) or row.get('arr_time') is None:
        reasons.append('NULL arr_time')

    if len(reasons) > 0:
        return False, '; '.join(reasons)
    return True, None

def apply_data_quality_checks(df, quarter):
    """Apply DQ checks - 100% validation on every row"""
    logger.info(f"Applying DQ checks to {quarter}: {len(df):,} records (100% validation)")

    dq_stats = {
        'total_records': len(df),
        'null_violations': 0,
        'duplicate_violations': 0
    }

    df['is_valid'] = True
    df['rejection_reason'] = None

    # Validate EVERY row
    validation_start = datetime.now()
    for idx, row in df.iterrows():
        is_valid, reason = validate_record(row)
        df.at[idx, 'is_valid'] = is_valid
        if reason:
            df.at[idx, 'rejection_reason'] = reason
            dq_stats['null_violations'] += 1

        # Progress logging every 50k validations
        if (idx + 1) % 50000 == 0:
            elapsed = (datetime.now() - validation_start).total_seconds()
            rate = (idx + 1) / elapsed
            remaining = (len(df) - idx - 1) / rate
            logger.info(f"Validation progress: {idx+1:,}/{len(df):,} ({(idx+1)/len(df)*100:.1f}%) - ETA: {remaining/60:.1f} min")

    validation_time = (datetime.now() - validation_start).total_seconds()
    logger.info(f"Validation completed in {validation_time:.1f} seconds")

    # Duplicate check
    df['_temp_key'] = (
        df['fl_date'].astype(str) + '_' + 
        df['op_unique_carrier'].astype(str) + '_' + 
        df['op_carrier_fl_num'].astype(str) + '_' + 
        df['origin'].astype(str) + '_' + 
        df['dest'].astype(str)
    )

    duplicate_mask = df.duplicated(subset=['_temp_key'], keep=False)
    dq_stats['duplicate_violations'] = duplicate_mask.sum()

    df.loc[duplicate_mask & df['is_valid'], 'is_valid'] = False
    df.loc[duplicate_mask & df['is_valid'], 'rejection_reason'] = 'Duplicate record'

    df = df.drop(columns=['_temp_key'])

    # Split clean vs quarantine
    clean_df = df[df['is_valid'] == True].copy()
    quarantine_df = df[df['is_valid'] == False].copy()

    # Add quarantine metadata
    if len(quarantine_df) > 0:
        quarantine_df['source_quarter'] = quarter
        quarantine_df['quarantine_date'] = datetime.now()

    clean_pct = (len(clean_df) / len(df) * 100) if len(df) > 0 else 0
    logger.info(f"{quarter} DQ Results: {len(clean_df):,} clean ({clean_pct:.2f}%), {len(quarantine_df):,} quarantined ({100-clean_pct:.2f}%)")

    # Check 70% threshold
    if clean_pct < MIN_CLEAN_DATA_PERCENTAGE:
        logger.error(f"QUALITY THRESHOLD VIOLATION: Only {clean_pct:.2f}% clean data (required: {MIN_CLEAN_DATA_PERCENTAGE}%)")
        raise ValueError(f"Data quality below acceptable threshold: {clean_pct:.2f}% < {MIN_CLEAN_DATA_PERCENTAGE}%")

    return clean_df, quarantine_df, dq_stats

# ============================================================
# DIMENSION LOADING
# ============================================================

def load_dim_date(conn):
    logger.info("Loading Dim_Date (only dates with flights)...")

    query = """
    SELECT DISTINCT 
        CAST(CONVERT(VARCHAR(8), fl_date, 112) AS INT) as date_key,
        fl_date as full_date,
        DATEPART(YEAR, fl_date) as year,
        DATEPART(QUARTER, fl_date) as quarter,
        DATEPART(MONTH, fl_date) as month,
        DATENAME(MONTH, fl_date) as month_name,
        DATEPART(DAY, fl_date) as day_of_month,
        DATEPART(WEEKDAY, fl_date) as day_of_week,
        DATENAME(WEEKDAY, fl_date) as day_name,
        CASE WHEN DATEPART(WEEKDAY, fl_date) IN (1, 7) THEN 1 ELSE 0 END as is_weekend
    FROM (
        SELECT fl_date FROM flight_analytics.dbo.Q1 WHERE fl_date IS NOT NULL
        UNION SELECT fl_date FROM flight_analytics.dbo.Q2 WHERE fl_date IS NOT NULL
        UNION SELECT fl_date FROM flight_analytics.dbo.Q3 WHERE fl_date IS NOT NULL
        UNION SELECT fl_date FROM flight_analytics.dbo.Q4 WHERE fl_date IS NOT NULL
    ) dates
    """

    source_conn = get_db_connection(SOURCE_CONN_STR)
    df = pd.read_sql(query, source_conn)
    source_conn.close()

    logger.info(f"Extracted {len(df):,} unique dates")
    bulk_insert(conn, 'Dim_Date', df)
    logger.info("Dim_Date loaded successfully")

def load_dim_airline(conn):
    logger.info("Loading Dim_Airline (with full names)...")

    query = """
    SELECT DISTINCT op_unique_carrier as carrier_code
    FROM (
        SELECT op_unique_carrier FROM flight_analytics.dbo.Q1 WHERE op_unique_carrier IS NOT NULL
        UNION SELECT op_unique_carrier FROM flight_analytics.dbo.Q2 WHERE op_unique_carrier IS NOT NULL
        UNION SELECT op_unique_carrier FROM flight_analytics.dbo.Q3 WHERE op_unique_carrier IS NOT NULL
        UNION SELECT op_unique_carrier FROM flight_analytics.dbo.Q4 WHERE op_unique_carrier IS NOT NULL
    ) carriers
    """

    source_conn = get_db_connection(SOURCE_CONN_STR)
    df = pd.read_sql(query, source_conn)
    source_conn.close()

    # Add full airline names
    df['carrier_name'] = df['carrier_code'].map(AIRLINE_NAMES)

    logger.info(f"Extracted {len(df):,} unique airlines")
    for idx, row in df.iterrows():
        logger.info(f"  {row['carrier_code']}: {row['carrier_name']}")

    bulk_insert(conn, 'Dim_Airline', df)
    logger.info("Dim_Airline loaded successfully")

def load_dim_airport(conn):
    """
    Load airport dimension - simplified for 25-column structure
    Since city/state columns were removed, we only get airport codes
    """
    logger.info("Loading Dim_Airport (airport codes only)...")

    query = """
    SELECT DISTINCT 
        airport_code,
        CAST(NULL AS VARCHAR(100)) as city_name,
        CAST(NULL AS VARCHAR(50)) as state_name
    FROM (
        SELECT origin as airport_code FROM flight_analytics.dbo.Q1 WHERE origin IS NOT NULL
        UNION
        SELECT dest FROM flight_analytics.dbo.Q1 WHERE dest IS NOT NULL
        UNION
        SELECT origin FROM flight_analytics.dbo.Q2 WHERE origin IS NOT NULL
        UNION
        SELECT dest FROM flight_analytics.dbo.Q2 WHERE dest IS NOT NULL
        UNION
        SELECT origin FROM flight_analytics.dbo.Q3 WHERE origin IS NOT NULL
        UNION
        SELECT dest FROM flight_analytics.dbo.Q3 WHERE dest IS NOT NULL
        UNION
        SELECT origin FROM flight_analytics.dbo.Q4 WHERE origin IS NOT NULL
        UNION
        SELECT dest FROM flight_analytics.dbo.Q4 WHERE dest IS NOT NULL
    ) airports
    """

    source_conn = get_db_connection(SOURCE_CONN_STR)
    df = pd.read_sql(query, source_conn)
    source_conn.close()

    logger.info(f"Extracted {len(df):,} unique airport codes")
    bulk_insert(conn, 'Dim_Airport', df)
    logger.info("Dim_Airport loaded successfully")

# ============================================================
# FACT LOADING
# ============================================================

def load_facts_for_quarter(quarter_name, target_conn):
    logger.info(f"{'='*80}")
    logger.info(f"Processing Quarter: {quarter_name}")
    logger.info(f"{'='*80}")

    # Extract only 25 columns
    cols_str = ', '.join(SELECT_COLUMNS)
    query = f"SELECT {cols_str} FROM flight_analytics.dbo.{quarter_name}"

    source_conn = get_db_connection(SOURCE_CONN_STR)
    logger.info(f"Extracting {len(SELECT_COLUMNS)} columns from {quarter_name}...")
    df = pd.read_sql(query, source_conn)
    source_conn.close()

    logger.info(f"Extracted {len(df):,} records")

    # Apply DQ checks (100% validation)
    clean_df, quarantine_df, dq_stats = apply_data_quality_checks(df, quarter_name)

    # Save quarantine records (summary only)
    if len(quarantine_df) > 0:
        logger.info(f"Saving {len(quarantine_df):,} quarantined records...")

        # Summary columns only
        quarantine_summary = quarantine_df[[
            'source_quarter', 'quarantine_date', 'rejection_reason',
            'fl_date', 'op_unique_carrier', 'op_carrier_fl_num', 'origin', 'dest'
        ]].copy()

        bulk_insert(target_conn, 'FlightData_Quarantine', quarantine_summary)
        logger.info("Quarantined records saved")

    # Check if we have clean data
    if len(clean_df) == 0:
        logger.error(f"ZERO clean records for {quarter_name}! Stopping ETL.")
        raise ValueError(f"No clean records in {quarter_name} - cannot continue")

    # Exclude cancelled flights
    original_count = len(clean_df)
    clean_df = clean_df[clean_df['cancelled'] != 1].copy()
    cancelled_count = original_count - len(clean_df)
    logger.info(f"Excluded {cancelled_count:,} cancelled flights, {len(clean_df):,} remaining")

    if len(clean_df) == 0:
        logger.error("All clean records were cancelled flights!")
        raise ValueError(f"No non-cancelled records in {quarter_name}")

    # Get dimension lookups
    logger.info("Building dimension key lookups...")
    date_lookup = pd.read_sql("SELECT date_key, full_date FROM Dim_Date", target_conn)
    date_lookup['full_date'] = pd.to_datetime(date_lookup['full_date'])
    airline_lookup = pd.read_sql("SELECT airline_key, carrier_code FROM Dim_Airline", target_conn)
    airport_lookup = pd.read_sql("SELECT airport_key, airport_code FROM Dim_Airport", target_conn)

    # Join FK lookups
    clean_df['fl_date'] = pd.to_datetime(clean_df['fl_date'])
    clean_df = clean_df.merge(date_lookup, left_on='fl_date', right_on='full_date', how='left')
    clean_df = clean_df.merge(airline_lookup, left_on='op_unique_carrier', right_on='carrier_code', how='left')
    clean_df = clean_df.merge(airport_lookup, left_on='origin', right_on='airport_code', how='left', suffixes=('', '_orig'))
    clean_df = clean_df.merge(airport_lookup, left_on='dest', right_on='airport_code', how='left', suffixes=('', '_dest'))

    clean_df.rename(columns={
        'airport_key': 'origin_airport_key',
        'airport_key_dest': 'dest_airport_key'
    }, inplace=True)

    # Remove rows without valid FKs
    clean_df = clean_df.dropna(subset=['date_key', 'airline_key', 'origin_airport_key', 'dest_airport_key'])
    logger.info(f"Records with valid FKs: {len(clean_df):,}")

    if len(clean_df) == 0:
        logger.error(f"No records with valid foreign keys for {quarter_name}!")
        raise ValueError(f"No valid FK matches in {quarter_name}")

    # Clean infinity/NaN from numeric columns BEFORE creating fact tables
    logger.info("Cleaning invalid float values...")
    numeric_cols = ['crs_dep_time', 'dep_time', 'crs_arr_time', 'arr_time',
                    'crs_elapsed_time', 'actual_elapsed_time', 'air_time',
                    'taxi_out', 'taxi_in', 'distance']

    for col in numeric_cols:
        if col in clean_df.columns:
            clean_df[col] = clean_df[col].replace([np.inf, -np.inf], None)
            clean_df[col] = clean_df[col].where(pd.notnull(clean_df[col]), None)

    # Load Fact_FlightPerformance
    logger.info("Loading Fact_FlightPerformance...")

    # Explicit conversion with safety checks
    fact_perf = pd.DataFrame({
        'date_key': clean_df['date_key'].astype(int),
        'airline_key': clean_df['airline_key'].astype(int),
        'origin_airport_key': clean_df['origin_airport_key'].astype(int),
        'dest_airport_key': clean_df['dest_airport_key'].astype(int),
        'flight_number': clean_df['op_carrier_fl_num'].astype(str),
        'scheduled_dep_time': pd.to_numeric(clean_df['crs_dep_time'], errors='coerce'),
        'actual_dep_time': pd.to_numeric(clean_df['dep_time'], errors='coerce'),
        'scheduled_arr_time': pd.to_numeric(clean_df['crs_arr_time'], errors='coerce'),
        'actual_arr_time': pd.to_numeric(clean_df['arr_time'], errors='coerce'),
        'scheduled_elapsed_time': pd.to_numeric(clean_df['crs_elapsed_time'], errors='coerce'),
        'actual_elapsed_time': pd.to_numeric(clean_df['actual_elapsed_time'], errors='coerce'),
        'air_time': pd.to_numeric(clean_df['air_time'], errors='coerce'),
        'taxi_out': pd.to_numeric(clean_df['taxi_out'], errors='coerce'),
        'taxi_in': pd.to_numeric(clean_df['taxi_in'], errors='coerce'),
        'distance': pd.to_numeric(clean_df['distance'], errors='coerce'),
        'cancelled': pd.to_numeric(clean_df['cancelled'], errors='coerce').astype('Int64'),
        'cancellation_code': clean_df['cancellation_code'].apply(lambda x: None if pd.isna(x) else str(x)[:10]),
        'diverted': pd.to_numeric(clean_df['diverted'], errors='coerce').astype('Int64')
    })

    # Replace any remaining invalid values
    fact_perf = fact_perf.replace([np.inf, -np.inf], None)

    bulk_insert(target_conn, 'Fact_FlightPerformance', fact_perf)

    # Load Fact_Delays with custom categories
    logger.info("Loading Fact_Delays...")

    def safe_sum_delays(row):
        delays = [row['carrier_delay'], row['weather_delay'], row['nas_delay'], 
                 row['security_delay'], row['late_aircraft_delay']]
        valid = [d for d in delays if pd.notna(d) and d is not None and not np.isinf(d)]
        return sum(valid) if valid else None

    def categorize_delay(delay):
        """Custom categories: On-Time (≤0), Minor (1-60), Moderate (61-180), Severe (>180)"""
        if pd.isna(delay) or delay is None or np.isinf(delay) or delay <= 0:
            return 'On-Time'
        elif delay <= 60:
            return 'Minor'
        elif delay <= 180:
            return 'Moderate'
        else:
            return 'Severe'

    fact_delays = pd.DataFrame({
        'date_key': clean_df['date_key'].astype(int),
        'airline_key': clean_df['airline_key'].astype(int),
        'origin_airport_key': clean_df['origin_airport_key'].astype(int),
        'dest_airport_key': clean_df['dest_airport_key'].astype(int),
        'flight_number': clean_df['op_carrier_fl_num'].astype(str),
        'departure_delay': clean_df['dep_delay'],
        'arrival_delay': clean_df['arr_delay'],
        'carrier_delay': clean_df['carrier_delay'],
        'weather_delay': clean_df['weather_delay'],
        'nas_delay': clean_df['nas_delay'],
        'security_delay': clean_df['security_delay'],
        'late_aircraft_delay': clean_df['late_aircraft_delay']
    })

    fact_delays['total_delay_minutes'] = clean_df.apply(safe_sum_delays, axis=1)
    fact_delays['is_delayed'] = fact_delays['arrival_delay'].apply(lambda x: 1 if pd.notna(x) and not np.isinf(x) and x > 15 else 0)
    fact_delays['delay_category'] = fact_delays['arrival_delay'].apply(categorize_delay)

    # Extra safety check
    fact_delays = fact_delays.replace([np.inf, -np.inf], None)

    bulk_insert(target_conn, 'Fact_Delays', fact_delays)

    # Save DQ metrics
    logger.info("Saving DQ metrics...")
    dq_record = pd.DataFrame([{
        'source_quarter': quarter_name,
        'total_records_processed': dq_stats['total_records'],
        'records_passed': len(clean_df),
        'records_quarantined': len(quarantine_df),
        'null_violations_count': dq_stats['null_violations'],
        'duplicate_violations_count': dq_stats['duplicate_violations'],
        'range_violations_count': 0,
        'format_violations_count': 0
    }])
    bulk_insert(target_conn, 'DQ_Metrics', dq_record)

    logger.info(f"{quarter_name} completed: {len(clean_df):,} loaded, {len(quarantine_df):,} quarantined")

# ============================================================
# MAIN ETL
# ============================================================

def main():
    start_time = datetime.now()
    logger.info("="*80)
    logger.info("FINAL ETL PIPELINE STARTED (Float Fix Applied)")
    logger.info(f"Start Time: {start_time}")
    logger.info("Configuration: 25 cols, 15 airlines, 100% DQ, >70% clean required")
    logger.info("="*80)

    try:
        target_conn = get_db_connection(TARGET_CONN_STR)

        # STEP 1: Dimensions
        logger.info("\n" + "="*80)
        logger.info("STEP 1: LOADING DIMENSION TABLES")
        logger.info("="*80)
        load_dim_date(target_conn)
        load_dim_airline(target_conn)
        load_dim_airport(target_conn)
        logger.info("All dimensions loaded successfully!")

        # STEP 2: Facts
        logger.info("\n" + "="*80)
        logger.info("STEP 2: LOADING FACT TABLES")
        logger.info("="*80)
        for quarter in ['Q1', 'Q2', 'Q3', 'Q4']:
            load_facts_for_quarter(quarter, target_conn)

        # STEP 3: Final Stats
        logger.info("\n" + "="*80)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*80)

        cursor = target_conn.cursor()

        cursor.execute("SELECT COUNT(*) FROM Dim_Date")
        date_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM Dim_Airline")
        airline_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM Dim_Airport")
        airport_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM Fact_FlightPerformance")
        perf_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM Fact_Delays")
        delay_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM FlightData_Quarantine")
        quarantine_count = cursor.fetchone()[0]
        cursor.execute("SELECT SUM(total_records_processed), SUM(records_passed), SUM(records_quarantined) FROM DQ_Metrics")
        dq_summary = cursor.fetchone()

        cursor.close()
        target_conn.close()

        end_time = datetime.now()
        duration = end_time - start_time

        logger.info("\nFINAL STATISTICS:")
        logger.info("-" * 80)
        logger.info("Dimensions:")
        logger.info(f"  Dim_Date: {date_count:,}")
        logger.info(f"  Dim_Airline: {airline_count:,}")
        logger.info(f"  Dim_Airport: {airport_count:,}")
        logger.info("\nFacts:")
        logger.info(f"  Fact_FlightPerformance: {perf_count:,}")
        logger.info(f"  Fact_Delays: {delay_count:,}")
        logger.info("\nData Quality:")
        logger.info(f"  Total Processed: {dq_summary[0]:,}")
        logger.info(f"  Clean: {dq_summary[1]:,} ({dq_summary[1]/dq_summary[0]*100:.2f}%)")
        logger.info(f"  Quarantined: {dq_summary[2]:,} ({dq_summary[2]/dq_summary[0]*100:.2f}%)")
        logger.info(f"\nExecution Time: {duration}")
        logger.info("="*80)
        logger.info("SUCCESS - ETL COMPLETED")
        logger.info("="*80)

    except Exception as e:
        logger.error(f"\n{'='*80}")
        logger.error(f"ETL PIPELINE FAILED: {e}")
        logger.error(f"{'='*80}")
        raise

if __name__ == "__main__":
    main()
