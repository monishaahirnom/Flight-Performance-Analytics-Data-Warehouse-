-- ============================================================
-- STEP 2: RECREATE Q1-Q4 TABLES WITH CORRECT DATA TYPES
-- Run this after backing up tables
-- ============================================================

USE flight_analytics;
GO

PRINT 'Dropping existing tables...'
DROP TABLE IF EXISTS Q1;
DROP TABLE IF EXISTS Q2;
DROP TABLE IF EXISTS Q3;
DROP TABLE IF EXISTS Q4;
GO

PRINT 'Creating Q1 with correct schema...'
CREATE TABLE Q1 (
    flight_id INT IDENTITY(1,1) PRIMARY KEY,
    fl_date DATE,
    op_unique_carrier VARCHAR(10),  -- CORRECT: VARCHAR not DECIMAL!
    op_carrier_fl_num VARCHAR(20),
    origin VARCHAR(10),
    dest VARCHAR(10),
    crs_dep_time INT,
    dep_time INT,
    crs_arr_time INT,
    arr_time INT,
    dep_delay FLOAT,
    arr_delay FLOAT,
    taxi_out FLOAT,
    taxi_in FLOAT,
    crs_elapsed_time FLOAT,
    actual_elapsed_time FLOAT,
    air_time FLOAT,
    distance FLOAT,
    cancelled INT,
    cancellation_code VARCHAR(5),
    diverted INT,
    carrier_delay FLOAT,
    weather_delay FLOAT,
    nas_delay FLOAT,
    security_delay FLOAT,
    late_aircraft_delay FLOAT
);

PRINT 'Creating Q2 with correct schema...'
CREATE TABLE Q2 (
    flight_id INT IDENTITY(1,1) PRIMARY KEY,
    fl_date DATE,
    op_unique_carrier VARCHAR(10),
    op_carrier_fl_num VARCHAR(20),
    origin VARCHAR(10),
    dest VARCHAR(10),
    crs_dep_time INT,
    dep_time INT,
    crs_arr_time INT,
    arr_time INT,
    dep_delay FLOAT,
    arr_delay FLOAT,
    taxi_out FLOAT,
    taxi_in FLOAT,
    crs_elapsed_time FLOAT,
    actual_elapsed_time FLOAT,
    air_time FLOAT,
    distance FLOAT,
    cancelled INT,
    cancellation_code VARCHAR(5),
    diverted INT,
    carrier_delay FLOAT,
    weather_delay FLOAT,
    nas_delay FLOAT,
    security_delay FLOAT,
    late_aircraft_delay FLOAT
);

PRINT 'Creating Q3 with correct schema...'
CREATE TABLE Q3 (
    flight_id INT IDENTITY(1,1) PRIMARY KEY,
    fl_date DATE,
    op_unique_carrier VARCHAR(10),
    op_carrier_fl_num VARCHAR(20),
    origin VARCHAR(10),
    dest VARCHAR(10),
    crs_dep_time INT,
    dep_time INT,
    crs_arr_time INT,
    arr_time INT,
    dep_delay FLOAT,
    arr_delay FLOAT,
    taxi_out FLOAT,
    taxi_in FLOAT,
    crs_elapsed_time FLOAT,
    actual_elapsed_time FLOAT,
    air_time FLOAT,
    distance FLOAT,
    cancelled INT,
    cancellation_code VARCHAR(5),
    diverted INT,
    carrier_delay FLOAT,
    weather_delay FLOAT,
    nas_delay FLOAT,
    security_delay FLOAT,
    late_aircraft_delay FLOAT
);

PRINT 'Creating Q4 with correct schema...'
CREATE TABLE Q4 (
    flight_id INT IDENTITY(1,1) PRIMARY KEY,
    fl_date DATE,
    op_unique_carrier VARCHAR(10),
    op_carrier_fl_num VARCHAR(20),
    origin VARCHAR(10),
    dest VARCHAR(10),
    crs_dep_time INT,
    dep_time INT,
    crs_arr_time INT,
    arr_time INT,
    dep_delay FLOAT,
    arr_delay FLOAT,
    taxi_out FLOAT,
    taxi_in FLOAT,
    crs_elapsed_time FLOAT,
    actual_elapsed_time FLOAT,
    air_time FLOAT,
    distance FLOAT,
    cancelled INT,
    cancellation_code VARCHAR(5),
    diverted INT,
    carrier_delay FLOAT,
    weather_delay FLOAT,
    nas_delay FLOAT,
    security_delay FLOAT,
    late_aircraft_delay FLOAT
);
GO

PRINT ''
PRINT '========================================'
PRINT 'TABLES RECREATED SUCCESSFULLY!'
PRINT 'Now import CSV files using SSMS wizard'
PRINT '========================================'
