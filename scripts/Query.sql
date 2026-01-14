-- ============================================================================
-- Normalized DB vs Star Schema Data Warehouse
-- ============================================================================

SET NOCOUNT ON;

-- Clean up any existing temp tables
IF OBJECT_ID('tempdb..#BenchmarkResults') IS NOT NULL DROP TABLE #BenchmarkResults;
IF OBJECT_ID('tempdb..#Q1_Normalized') IS NOT NULL DROP TABLE #Q1_Normalized;
IF OBJECT_ID('tempdb..#Q2_Normalized') IS NOT NULL DROP TABLE #Q2_Normalized;
IF OBJECT_ID('tempdb..#Q3_Normalized') IS NOT NULL DROP TABLE #Q3_Normalized;

-- Create results table
CREATE TABLE #BenchmarkResults (
    QueryNumber INT,
    QueryName VARCHAR(200),
    DatabaseType VARCHAR(50),
    ExecutionTime_ms INT,
    ExecutionTime_sec DECIMAL(10,2),
    LinesOfCode INT,
    ResultRows INT
);

PRINT '============================================================================';
PRINT 'Comparison: Normalized (Q1-Q4 tables) vs Star Schema (Dimensional Model)';
PRINT '============================================================================';

-- ============================================================================
-- QUERY 1: Which carriers have the best on-time performance by route?
-- Business Value: Partner selection, codeshare agreements, gate allocation
-- ============================================================================

PRINT '';
PRINT '--- QUERY 1: Best Carriers by Route (On-Time Performance) ---';
PRINT 'Business Question: Which carriers should we partner with for reliable service?';
PRINT '';

-- Normalized DB
PRINT 'Executing on Normalized DB (flight_analytics)...';
USE flight_analytics;

DECLARE @Start1N DATETIME = GETDATE();

SELECT TOP 20
    origin,
    dest,
    op_unique_carrier AS carrier,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN arr_delay <= 0 THEN 1 ELSE 0 END) AS on_time_flights,
    CAST(ROUND(100.0 * SUM(CASE WHEN arr_delay <= 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS DECIMAL(5,2)) AS on_time_pct,
    ROUND(AVG(arr_delay), 2) AS avg_delay_minutes
INTO #Q1_Normalized
FROM (
    SELECT origin, dest, op_unique_carrier, arr_delay, cancelled 
    FROM dbo.Q1 WHERE cancelled = 0
    UNION ALL
    SELECT origin, dest, op_unique_carrier, arr_delay, cancelled 
    FROM dbo.Q2 WHERE cancelled = 0
    UNION ALL
    SELECT origin, dest, op_unique_carrier, arr_delay, cancelled 
    FROM dbo.Q3 WHERE cancelled = 0
    UNION ALL
    SELECT origin, dest, op_unique_carrier, arr_delay, cancelled 
    FROM dbo.Q4 WHERE cancelled = 0
) AS all_flights
WHERE arr_delay IS NOT NULL
GROUP BY origin, dest, op_unique_carrier
HAVING COUNT(*) >= 500
ORDER BY on_time_pct DESC, total_flights DESC;

DECLARE @End1N DATETIME = GETDATE();
DECLARE @Rows1N INT = (SELECT COUNT(*) FROM #Q1_Normalized);

INSERT INTO #BenchmarkResults (QueryNumber, QueryName, DatabaseType, ExecutionTime_ms, ExecutionTime_sec, LinesOfCode, ResultRows)
VALUES (
    1, 
    'Best Carriers by Route (On-Time Performance)', 
    'Normalized', 
    DATEDIFF(MILLISECOND, @Start1N, @End1N),
    CAST(DATEDIFF(MILLISECOND, @Start1N, @End1N) / 1000.0 AS DECIMAL(10,2)),
    18,
    @Rows1N
);

PRINT 'Normalized DB: ' + CAST(CAST(DATEDIFF(MILLISECOND, @Start1N, @End1N) / 1000.0 AS DECIMAL(10,2)) AS VARCHAR) + ' seconds';
PRINT 'Result rows: ' + CAST(@Rows1N AS VARCHAR);
PRINT '';

-- Star Schema
PRINT 'Executing on Star Schema (FlightDataWarehouse)...';
USE FlightDataWarehouse;

DECLARE @Start1S DATETIME = GETDATE();
DECLARE @Rows1S INT = 20; -- TOP 20 results

-- Execute the query
SELECT TOP 20
    orig.airport_code AS origin,
    dest_apt.airport_code AS destination,
    a.carrier_code,
    a.carrier_name,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN d.arrival_delay <= 0 THEN 1 ELSE 0 END) AS on_time_flights,
    ROUND(100.0 * SUM(CASE WHEN d.arrival_delay <= 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS on_time_pct,
    ROUND(AVG(d.arrival_delay), 2) AS avg_delay_minutes
FROM dbo.Fact_Delays d
INNER JOIN dbo.Dim_Airport orig ON d.origin_airport_key = orig.airport_key
INNER JOIN dbo.Dim_Airport dest_apt ON d.dest_airport_key = dest_apt.airport_key
INNER JOIN dbo.Dim_Airline a ON d.airline_key = a.airline_key
WHERE d.arrival_delay IS NOT NULL
GROUP BY orig.airport_code, dest_apt.airport_code, a.carrier_code, a.carrier_name
HAVING COUNT(*) >= 500
ORDER BY on_time_pct DESC, total_flights DESC;

DECLARE @End1S DATETIME = GETDATE();

INSERT INTO #BenchmarkResults (QueryNumber, QueryName, DatabaseType, ExecutionTime_ms, ExecutionTime_sec, LinesOfCode, ResultRows)
VALUES (
    1, 
    'Best Carriers by Route (On-Time Performance)', 
    'Star Schema', 
    DATEDIFF(MILLISECOND, @Start1S, @End1S),
    CAST(DATEDIFF(MILLISECOND, @Start1S, @End1S) / 1000.0 AS DECIMAL(10,2)),
    21,
    @Rows1S
);

PRINT 'Star Schema: ' + CAST(CAST(DATEDIFF(MILLISECOND, @Start1S, @End1S) / 1000.0 AS DECIMAL(10,2)) AS VARCHAR) + ' seconds';
PRINT 'Result rows: ' + CAST(@Rows1S AS VARCHAR);
PRINT 'Speedup: ' + CAST(CAST(DATEDIFF(MILLISECOND, @Start1N, @End1N) * 1.0 / NULLIF(DATEDIFF(MILLISECOND, @Start1S, @End1S), 0) AS DECIMAL(10,2)) AS VARCHAR) + 'x';
PRINT '';
PRINT '--- Query 1 Complete ---';
PRINT '';

-- ============================================================================
-- QUERY 2: What's the breakdown of delay causes by carrier?
-- Business Value: Root cause analysis, investment prioritization, accountability
-- ============================================================================

PRINT '';
PRINT '--- QUERY 2: Delay Cause Breakdown by Carrier ---';
PRINT 'Business Question: Are our delays controllable or caused by external factors?';
PRINT '';

-- Normalized DB
PRINT 'Executing on Normalized DB (flight_analytics)...';
USE flight_analytics;

DECLARE @Start2N DATETIME = GETDATE();

SELECT 
    op_unique_carrier AS carrier,
    COUNT(*) AS total_delayed_flights,
    ROUND(AVG(arr_delay), 2) AS avg_total_delay,
    ROUND(AVG(carrier_delay), 2) AS avg_carrier_delay,
    ROUND(AVG(weather_delay), 2) AS avg_weather_delay,
    ROUND(AVG(nas_delay), 2) AS avg_nas_delay,
    ROUND(AVG(security_delay), 2) AS avg_security_delay,
    ROUND(AVG(late_aircraft_delay), 2) AS avg_late_aircraft_delay
INTO #Q2_Normalized
FROM (
    SELECT op_unique_carrier, arr_delay, carrier_delay, weather_delay, 
           nas_delay, security_delay, late_aircraft_delay
    FROM dbo.Q1 WHERE cancelled = 0 AND arr_delay > 0
    UNION ALL
    SELECT op_unique_carrier, arr_delay, carrier_delay, weather_delay, 
           nas_delay, security_delay, late_aircraft_delay
    FROM dbo.Q2 WHERE cancelled = 0 AND arr_delay > 0
    UNION ALL
    SELECT op_unique_carrier, arr_delay, carrier_delay, weather_delay, 
           nas_delay, security_delay, late_aircraft_delay
    FROM dbo.Q3 WHERE cancelled = 0 AND arr_delay > 0
    UNION ALL
    SELECT op_unique_carrier, arr_delay, carrier_delay, weather_delay, 
           nas_delay, security_delay, late_aircraft_delay
    FROM dbo.Q4 WHERE cancelled = 0 AND arr_delay > 0
) AS all_flights
WHERE arr_delay IS NOT NULL
GROUP BY op_unique_carrier
ORDER BY total_delayed_flights DESC;

DECLARE @End2N DATETIME = GETDATE();
DECLARE @Rows2N INT = (SELECT COUNT(*) FROM #Q2_Normalized);

INSERT INTO #BenchmarkResults (QueryNumber, QueryName, DatabaseType, ExecutionTime_ms, ExecutionTime_sec, LinesOfCode, ResultRows)
VALUES (
    2, 
    'Delay Cause Breakdown by Carrier', 
    'Normalized', 
    DATEDIFF(MILLISECOND, @Start2N, @End2N),
    CAST(DATEDIFF(MILLISECOND, @Start2N, @End2N) / 1000.0 AS DECIMAL(10,2)),
    26,
    @Rows2N
);

PRINT 'Normalized DB: ' + CAST(CAST(DATEDIFF(MILLISECOND, @Start2N, @End2N) / 1000.0 AS DECIMAL(10,2)) AS VARCHAR) + ' seconds';
PRINT 'Result rows: ' + CAST(@Rows2N AS VARCHAR);
PRINT '';

-- Star Schema
PRINT 'Executing on Star Schema (FlightDataWarehouse)...';
USE FlightDataWarehouse;

DECLARE @Start2S DATETIME = GETDATE();

SELECT 
    a.carrier_code,
    a.carrier_name,
    COUNT(*) AS total_delayed_flights,
    ROUND(AVG(d.arrival_delay), 2) AS avg_total_delay,
    ROUND(AVG(d.carrier_delay), 2) AS avg_carrier_delay,
    ROUND(AVG(d.weather_delay), 2) AS avg_weather_delay,
    ROUND(AVG(d.nas_delay), 2) AS avg_nas_delay,
    ROUND(AVG(d.security_delay), 2) AS avg_security_delay,
    ROUND(AVG(d.late_aircraft_delay), 2) AS avg_late_aircraft_delay
FROM dbo.Fact_Delays d
INNER JOIN dbo.Dim_Airline a ON d.airline_key = a.airline_key
WHERE d.is_delayed = 1 AND d.arrival_delay > 0
GROUP BY a.carrier_code, a.carrier_name
ORDER BY total_delayed_flights DESC;

DECLARE @End2S DATETIME = GETDATE();
DECLARE @Rows2S INT = @@ROWCOUNT;

INSERT INTO #BenchmarkResults (QueryNumber, QueryName, DatabaseType, ExecutionTime_ms, ExecutionTime_sec, LinesOfCode, ResultRows)
VALUES (
    2, 
    'Delay Cause Breakdown by Carrier', 
    'Star Schema', 
    DATEDIFF(MILLISECOND, @Start2S, @End2S),
    CAST(DATEDIFF(MILLISECOND, @Start2S, @End2S) / 1000.0 AS DECIMAL(10,2)),
    20,
    @Rows2S
);

PRINT 'Star Schema: ' + CAST(CAST(DATEDIFF(MILLISECOND, @Start2S, @End2S) / 1000.0 AS DECIMAL(10,2)) AS VARCHAR) + ' seconds';
PRINT 'Result rows: ' + CAST(@Rows2S AS VARCHAR);
PRINT 'Speedup: ' + CAST(CAST(DATEDIFF(MILLISECOND, @Start2N, @End2N) * 1.0 / NULLIF(DATEDIFF(MILLISECOND, @Start2S, @End2S), 0) AS DECIMAL(10,2)) AS VARCHAR) + 'x';
PRINT '';
PRINT '--- Query 2 Complete ---';
PRINT '';

-- ============================================================================
-- QUERY 3: Which airports cause the most departure delays?
-- Business Value: Capacity planning, resource allocation, schedule optimization
-- ============================================================================

PRINT '';
PRINT '--- QUERY 3: Airports with Most Departure Delays ---';
PRINT 'Business Question: Where should we add resources to reduce congestion?';
PRINT '';

-- Normalized DB
PRINT 'Executing on Normalized DB (flight_analytics)...';
USE flight_analytics;

DECLARE @Start3N DATETIME = GETDATE();

SELECT TOP 25
    origin AS airport_code,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN dep_delay > 15 THEN 1 ELSE 0 END) AS delayed_departures,
    CAST(ROUND(100.0 * SUM(CASE WHEN dep_delay > 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS DECIMAL(5,2)) AS delay_rate_pct,
    ROUND(AVG(dep_delay), 2) AS avg_departure_delay
INTO #Q3_Normalized
FROM (
    SELECT origin, dep_delay, cancelled FROM dbo.Q1 WHERE cancelled = 0
    UNION ALL
    SELECT origin, dep_delay, cancelled FROM dbo.Q2 WHERE cancelled = 0
    UNION ALL
    SELECT origin, dep_delay, cancelled FROM dbo.Q3 WHERE cancelled = 0
    UNION ALL
    SELECT origin, dep_delay, cancelled FROM dbo.Q4 WHERE cancelled = 0
) AS all_flights
WHERE dep_delay IS NOT NULL
GROUP BY origin
HAVING COUNT(*) >= 1000
ORDER BY delayed_departures DESC;

DECLARE @End3N DATETIME = GETDATE();
DECLARE @Rows3N INT = (SELECT COUNT(*) FROM #Q3_Normalized);

INSERT INTO #BenchmarkResults (QueryNumber, QueryName, DatabaseType, ExecutionTime_ms, ExecutionTime_sec, LinesOfCode, ResultRows)
VALUES (
    3, 
    'Airports with Most Departure Delays', 
    'Normalized', 
    DATEDIFF(MILLISECOND, @Start3N, @End3N),
    CAST(DATEDIFF(MILLISECOND, @Start3N, @End3N) / 1000.0 AS DECIMAL(10,2)),
    15,
    @Rows3N
);

PRINT 'Normalized DB: ' + CAST(CAST(DATEDIFF(MILLISECOND, @Start3N, @End3N) / 1000.0 AS DECIMAL(10,2)) AS VARCHAR) + ' seconds';
PRINT 'Result rows: ' + CAST(@Rows3N AS VARCHAR);
PRINT '';

-- Star Schema
PRINT 'Executing on Star Schema (FlightDataWarehouse)...';
USE FlightDataWarehouse;

DECLARE @Start3S DATETIME = GETDATE();

SELECT TOP 25
    apt.airport_code,
    COUNT(*) AS total_flights,
    SUM(CASE WHEN d.departure_delay > 15 THEN 1 ELSE 0 END) AS delayed_departures,
    ROUND(100.0 * SUM(CASE WHEN d.departure_delay > 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS delay_rate_pct,
    ROUND(AVG(d.departure_delay), 2) AS avg_departure_delay,
    ROUND(AVG(CASE WHEN d.departure_delay > 0 THEN d.departure_delay END), 2) AS avg_delay_when_delayed,
    MAX(d.departure_delay) AS max_departure_delay
FROM dbo.Fact_Delays d
INNER JOIN dbo.Dim_Airport apt ON d.origin_airport_key = apt.airport_key
WHERE d.departure_delay IS NOT NULL
GROUP BY apt.airport_code
HAVING COUNT(*) >= 1000
ORDER BY delayed_departures DESC;

DECLARE @End3S DATETIME = GETDATE();
DECLARE @Rows3S INT = @@ROWCOUNT;

INSERT INTO #BenchmarkResults (QueryNumber, QueryName, DatabaseType, ExecutionTime_ms, ExecutionTime_sec, LinesOfCode, ResultRows)
VALUES (
    3, 
    'Airports with Most Departure Delays', 
    'Star Schema', 
    DATEDIFF(MILLISECOND, @Start3S, @End3S),
    CAST(DATEDIFF(MILLISECOND, @Start3S, @End3S) / 1000.0 AS DECIMAL(10,2)),
    15,
    @Rows3S
);

PRINT 'Star Schema: ' + CAST(CAST(DATEDIFF(MILLISECOND, @Start3S, @End3S) / 1000.0 AS DECIMAL(10,2)) AS VARCHAR) + ' seconds';
PRINT 'Result rows: ' + CAST(@Rows3S AS VARCHAR);
PRINT 'Speedup: ' + CAST(CAST(DATEDIFF(MILLISECOND, @Start3N, @End3N) * 1.0 / NULLIF(DATEDIFF(MILLISECOND, @Start3S, @End3S), 0) AS DECIMAL(10,2)) AS VARCHAR) + 'x';
PRINT '';
PRINT '--- Query 3 Complete ---';
PRINT '';

USE flight_analytics;

PRINT '============================================================================';
PRINT '                    PERFORMANCE BENCHMARK RESULTS                           ';
PRINT '============================================================================';

-- Side-by-Side Comparison
PRINT '--- Performance Comparison Table ---';
PRINT '';

WITH comparison AS (
    SELECT 
        n.QueryNumber,
        LEFT(n.QueryName, 45) AS QueryName,
        n.ExecutionTime_sec AS Normalized_sec,
        s.ExecutionTime_sec AS StarSchema_sec,
        CAST((n.ExecutionTime_sec * 1.0 / NULLIF(s.ExecutionTime_sec, 0)) AS DECIMAL(10,2)) AS Speedup,
        CAST((1.0 - (s.ExecutionTime_sec / NULLIF(n.ExecutionTime_sec, 0))) * 100 AS DECIMAL(10,1)) AS Improvement_Pct
    FROM #BenchmarkResults n
    INNER JOIN #BenchmarkResults s 
        ON n.QueryNumber = s.QueryNumber 
        AND n.DatabaseType = 'Normalized' 
        AND s.DatabaseType = 'Star Schema'
)
SELECT 
    QueryNumber AS [Q#],
    QueryName AS [Query],
    Normalized_sec AS [Normalized (sec)],
    StarSchema_sec AS [Star Schema (sec)],
    Speedup AS [Speedup],
    CAST(Improvement_Pct AS VARCHAR) + '%' AS [Improvement]
FROM comparison
ORDER BY QueryNumber;

PRINT '';
PRINT '--- Summary Statistics ---';
PRINT '';

DECLARE @TotalNorm DECIMAL(10,2) = (SELECT SUM(ExecutionTime_sec) FROM #BenchmarkResults WHERE DatabaseType = 'Normalized');
DECLARE @TotalStar DECIMAL(10,2) = (SELECT SUM(ExecutionTime_sec) FROM #BenchmarkResults WHERE DatabaseType = 'Star Schema');
DECLARE @OverallSpeedup DECIMAL(10,2) = @TotalNorm / NULLIF(@TotalStar, 0);
DECLARE @TimeReduction DECIMAL(10,2) = ((@TotalNorm - @TotalStar) / NULLIF(@TotalNorm, 0)) * 100;

PRINT 'Total Execution Time:';
PRINT '  Normalized DB:  ' + CAST(@TotalNorm AS VARCHAR) + ' seconds';
PRINT '  Star Schema:    ' + CAST(@TotalStar AS VARCHAR) + ' seconds';
PRINT '  Time Saved:     ' + CAST(@TotalNorm - @TotalStar AS VARCHAR) + ' seconds';
PRINT '';
PRINT 'Overall Performance:';
PRINT '  Average Speedup: ' + CAST(@OverallSpeedup AS VARCHAR) + 'x faster';
PRINT '  Improvement:     ' + CAST(@TimeReduction AS VARCHAR) + '% reduction in query time';
PRINT '';

PRINT '============================================================================';
PRINT '                         BENCHMARK COMPLETE                                 ';
PRINT '============================================================================';

-- Cleanup
DROP TABLE #BenchmarkResults;
DROP TABLE #Q1_Normalized;
DROP TABLE #Q2_Normalized;
DROP TABLE #Q3_Normalized;

PRINT '';
PRINT 'Script complete. Temp tables cleaned up.';