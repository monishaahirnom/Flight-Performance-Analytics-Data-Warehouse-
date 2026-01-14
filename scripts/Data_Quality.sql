-- Quarantine table and DQ metrics tracking
USE FlightDataWarehouse;
GO

-- QUARANTINE TABLE
CREATE TABLE FlightData_Quarantine (
    quarantine_id BIGINT IDENTITY(1,1) PRIMARY KEY,
    quarantine_date DATETIME DEFAULT GETDATE(),
    source_quarter VARCHAR(10),
    rejection_reason VARCHAR(500),

    -- Original flight data columns (all 35 fields preserved)
    year SMALLINT,
    month TINYINT,
    day_of_month TINYINT,
    day_of_week TINYINT,
    fl_date DATE,
    op_unique_carrier VARCHAR(10),
    op_carrier_fl_num VARCHAR(20),
    origin VARCHAR(10),
    origin_city_name VARCHAR(100),
    origin_state_nm VARCHAR(50),
    dest VARCHAR(10),
    dest_city_name VARCHAR(100),
    dest_state_nm VARCHAR(50),
    crs_dep_time SMALLINT,
    dep_time FLOAT,
    dep_delay FLOAT,
    taxi_out FLOAT,
    wheels_off FLOAT,
    wheels_on FLOAT,
    taxi_in FLOAT,
    crs_arr_time SMALLINT,
    arr_time FLOAT,
    arr_delay FLOAT,
    cancelled TINYINT,
    cancellation_code VARCHAR(1),
    diverted SMALLINT,
    crs_elapsed_time FLOAT,
    actual_elapsed_time FLOAT,
    air_time FLOAT,
    distance FLOAT,
    carrier_delay SMALLINT,
    weather_delay SMALLINT,
    nas_delay SMALLINT,
    security_delay SMALLINT,
    late_aircraft_delay SMALLINT
);
GO

-- Index for querying quarantined records
CREATE NONCLUSTERED INDEX IX_Quarantine_Date ON FlightData_Quarantine(quarantine_date);
CREATE NONCLUSTERED INDEX IX_Quarantine_Quarter ON FlightData_Quarantine(source_quarter);
CREATE NONCLUSTERED INDEX IX_Quarantine_Reason ON FlightData_Quarantine(rejection_reason);
GO


-- DATA QUALITY METRICS TABLE
CREATE TABLE DQ_Metrics (
    metric_id INT IDENTITY(1,1) PRIMARY KEY,
    etl_run_date DATETIME DEFAULT GETDATE(),
    source_quarter VARCHAR(10),

    -- Record counts
    total_records_processed INT,
    records_passed INT,
    records_quarantined INT,

    -- Violation counts by type
    null_violations_count INT,
    duplicate_violations_count INT,
    range_violations_count INT,
    format_violations_count INT
);
GO

-- Index for reporting
CREATE NONCLUSTERED INDEX IX_DQ_Metrics_Date ON DQ_Metrics(etl_run_date);
CREATE NONCLUSTERED INDEX IX_DQ_Metrics_Quarter ON DQ_Metrics(source_quarter);
GO

PRINT 'Data Quality tables created successfully!';
GO