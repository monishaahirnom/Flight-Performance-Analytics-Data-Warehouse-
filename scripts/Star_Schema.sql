-- Create dimensional model with Fact and Dimension tables
CREATE DATABASE FlightDataWarehouse;
GO

USE FlightDataWarehouse;
GO

-- DIMENSION TABLES
-- Dim_Date: Time dimension for date-based analysis
CREATE TABLE Dim_Date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    year SMALLINT NOT NULL,
    quarter TINYINT NOT NULL,
    month TINYINT NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    day_of_month TINYINT NOT NULL,
    day_of_week TINYINT NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BIT NOT NULL
);
GO

-- Dim_Airline: Carrier dimension
CREATE TABLE Dim_Airline (
    airline_key INT IDENTITY(1,1) PRIMARY KEY,
    carrier_code VARCHAR(10) NOT NULL UNIQUE,
    carrier_name VARCHAR(100)
);
GO

-- Dim_Airport: Airport dimension (combined origin and destination)
CREATE TABLE Dim_Airport (
    airport_key INT IDENTITY(1,1) PRIMARY KEY,
    airport_code VARCHAR(10) NOT NULL UNIQUE,
    city_name VARCHAR(100),
    state_name VARCHAR(50)
);
GO


-- FACT TABLES (Multi-Fact Design - Complexity D)
-- Fact_FlightPerformance: Main operational metrics
CREATE TABLE Fact_FlightPerformance (
    flight_performance_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT NOT NULL,
    airline_key INT NOT NULL,
    origin_airport_key INT NOT NULL,
    dest_airport_key INT NOT NULL,

    -- Flight identifiers
    flight_number VARCHAR(20),

    -- Time metrics (in minutes)
    scheduled_dep_time SMALLINT,
    actual_dep_time FLOAT,
    scheduled_arr_time SMALLINT,
    actual_arr_time FLOAT,
    scheduled_elapsed_time FLOAT,
    actual_elapsed_time FLOAT,
    air_time FLOAT,
    taxi_out FLOAT,
    taxi_in FLOAT,

    -- Operational metrics
    distance FLOAT,
    cancelled TINYINT,
    cancellation_code VARCHAR(1),
    diverted SMALLINT,

    -- Foreign Keys
    CONSTRAINT FK_FlightPerf_Date FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT FK_FlightPerf_Airline FOREIGN KEY (airline_key) REFERENCES Dim_Airline(airline_key),
    CONSTRAINT FK_FlightPerf_Origin FOREIGN KEY (origin_airport_key) REFERENCES Dim_Airport(airport_key),
    CONSTRAINT FK_FlightPerf_Dest FOREIGN KEY (dest_airport_key) REFERENCES Dim_Airport(airport_key)
);
GO

-- Fact_Delays: Separate fact for delay analysis (Complexity D)
CREATE TABLE Fact_Delays (
    delay_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    date_key INT NOT NULL,
    airline_key INT NOT NULL,
    origin_airport_key INT NOT NULL,
    dest_airport_key INT NOT NULL,

    -- Flight identifiers (for joining back to performance if needed)
    flight_number VARCHAR(20),

    -- Delay metrics (in minutes)
    departure_delay FLOAT,
    arrival_delay FLOAT,

    -- Delay breakdown
    carrier_delay SMALLINT,
    weather_delay SMALLINT,
    nas_delay SMALLINT,
    security_delay SMALLINT,
    late_aircraft_delay SMALLINT,

    -- Derived metrics
    total_delay_minutes FLOAT,
    is_delayed BIT, -- 1 if arr_delay > 15 minutes
    delay_category VARCHAR(20), -- 'On-Time', 'Minor', 'Moderate', 'Severe'

    -- Foreign Keys
    CONSTRAINT FK_Delays_Date FOREIGN KEY (date_key) REFERENCES Dim_Date(date_key),
    CONSTRAINT FK_Delays_Airline FOREIGN KEY (airline_key) REFERENCES Dim_Airline(airline_key),
    CONSTRAINT FK_Delays_Origin FOREIGN KEY (origin_airport_key) REFERENCES Dim_Airport(airport_key),
    CONSTRAINT FK_Delays_Dest FOREIGN KEY (dest_airport_key) REFERENCES Dim_Airport(airport_key)
);
GO


-- INDEXES FOR STAR SCHEMA (Optimized for Analytics)
-- Fact_FlightPerformance indexes
CREATE NONCLUSTERED INDEX IX_FlightPerf_Date ON Fact_FlightPerformance(date_key);
CREATE NONCLUSTERED INDEX IX_FlightPerf_Airline ON Fact_FlightPerformance(airline_key);
CREATE NONCLUSTERED INDEX IX_FlightPerf_Origin ON Fact_FlightPerformance(origin_airport_key);
CREATE NONCLUSTERED INDEX IX_FlightPerf_Dest ON Fact_FlightPerformance(dest_airport_key);
CREATE NONCLUSTERED INDEX IX_FlightPerf_Composite ON Fact_FlightPerformance(date_key, airline_key);
GO

-- Fact_Delays indexes
CREATE NONCLUSTERED INDEX IX_Delays_Date ON Fact_Delays(date_key);
CREATE NONCLUSTERED INDEX IX_Delays_Airline ON Fact_Delays(airline_key);
CREATE NONCLUSTERED INDEX IX_Delays_Origin ON Fact_Delays(origin_airport_key);
CREATE NONCLUSTERED INDEX IX_Delays_Dest ON Fact_Delays(dest_airport_key);
CREATE NONCLUSTERED INDEX IX_Delays_Category ON Fact_Delays(delay_category);
CREATE NONCLUSTERED INDEX IX_Delays_Composite ON Fact_Delays(date_key, airline_key);
GO

PRINT 'Star Schema Data Warehouse created successfully!';
GO