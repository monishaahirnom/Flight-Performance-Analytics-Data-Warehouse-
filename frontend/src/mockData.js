// Predefined Query Definitions
export const MOCK_PREDEFINED_QUERIES = [
  {
    id: 1,
    name: 'Best Carriers by Route (On-Time Performance)',
    description: 'Which carriers have the best on-time performance by route? Uses star schema for fast aggregation.',
    sql: `SELECT TOP 10
      dc.carrier_name,
      do.origin_airport_code + '-' + dd.dest_airport_code AS route,
      COUNT(*) AS total_flights,
      AVG(CASE WHEN df.arrival_delay_minutes <= 0 THEN 1.0 ELSE 0.0 END) * 100 AS on_time_pct,
      AVG(df.arrival_delay_minutes) AS avg_delay_minutes
    FROM fact_flights ff
    JOIN dim_carrier dc ON ff.carrier_key = dc.carrier_key
    JOIN dim_origin do ON ff.origin_key = do.origin_key
    JOIN dim_destination dd ON ff.destination_key = dd.destination_key
    JOIN dim_flight_details df ON ff.flight_details_key = df.flight_details_key
    WHERE ff.cancelled = 0
    GROUP BY dc.carrier_name, do.origin_airport_code, dd.dest_airport_code
    HAVING COUNT(*) > 100
    ORDER BY on_time_pct DESC, total_flights DESC;`,
    mockDataKey: 'route_performance'
  },
  {
    id: 2,
    name: 'Delay Cause Breakdown by Carrier',
    description: 'Root cause analysis of delays by carrier. Identifies whether delays are carrier-controlled or external.',
    sql: `SELECT TOP 15
      dc.carrier_name,
      COUNT(*) AS total_flights,
      AVG(df.carrier_delay_minutes) AS avg_carrier_delay,
      AVG(df.weather_delay_minutes) AS avg_weather_delay,
      AVG(df.nas_delay_minutes) AS avg_nas_delay,
      AVG(df.security_delay_minutes) AS avg_security_delay,
      AVG(df.late_aircraft_delay_minutes) AS avg_late_aircraft_delay,
      AVG(df.arrival_delay_minutes) AS avg_total_delay
    FROM fact_flights ff
    JOIN dim_carrier dc ON ff.carrier_key = dc.carrier_key
    JOIN dim_flight_details df ON ff.flight_details_key = df.flight_details_key
    WHERE ff.cancelled = 0 AND df.arrival_delay_minutes > 0
    GROUP BY dc.carrier_name
    ORDER BY avg_total_delay DESC;`,
    mockDataKey: 'delay_breakdown'
  },
  {
    id: 3,
    name: 'Airports with Most Departure Delays',
    description: 'Which airports need more resources? Identifies bottleneck airports with consistent departure delays.',
    sql: `SELECT TOP 20
      do.origin_airport_code,
      do.origin_city,
      do.origin_state,
      COUNT(*) AS total_departures,
      AVG(df.departure_delay_minutes) AS avg_departure_delay,
      SUM(CASE WHEN df.departure_delay_minutes > 15 THEN 1 ELSE 0 END) AS flights_delayed_15min,
      SUM(CASE WHEN ff.cancelled = 1 THEN 1 ELSE 0 END) AS cancelled_flights
    FROM fact_flights ff
    JOIN dim_origin do ON ff.origin_key = do.origin_key
    JOIN dim_flight_details df ON ff.flight_details_key = df.flight_details_key
    GROUP BY do.origin_airport_code, do.origin_city, do.origin_state
    HAVING COUNT(*) > 1000
    ORDER BY avg_departure_delay DESC;`,
    mockDataKey: 'airport_delays'
  },
  {
    id: 4,
    name: 'Complete Carrier Performance Scorecard',
    description: 'Comprehensive metrics across all dimensions. This query takes 30+ seconds on normalized DB vs 8 seconds on warehouse.',
    sql: `SELECT
      dc.carrier_name,
      COUNT(*) AS total_flights,
      AVG(df.air_time_minutes) AS avg_air_time,
      AVG(df.distance_miles) AS avg_distance,
      AVG(df.departure_delay_minutes) AS avg_dep_delay,
      AVG(df.arrival_delay_minutes) AS avg_arr_delay,
      SUM(CASE WHEN ff.cancelled = 1 THEN 1 ELSE 0 END) AS total_cancelled,
      AVG(CASE WHEN df.arrival_delay_minutes <= 0 THEN 1.0 ELSE 0.0 END) * 100 AS on_time_pct
    FROM fact_flights ff
    JOIN dim_carrier dc ON ff.carrier_key = dc.carrier_key
    JOIN dim_flight_details df ON ff.flight_details_key = df.flight_details_key
    GROUP BY dc.carrier_name
    ORDER BY on_time_pct DESC;`,
    mockDataKey: 'carrier_scorecard'
  }
];

// ============================================
// QUERY 1: Best Carriers by Route (On-Time Performance)
// Fields: origin, destination, carrier_code, carrier_name, total_flights, on_time_flights, on_time_pct, avg_delay_minutes
// ============================================
export const MOCK_ROUTE_PERFORMANCE = [
  { origin: "TYS", destination: "DTW", carrier_code: "9E", carrier_name: "Endeavor Air", total_flights: 560, on_time_flights: 503, on_time_pct: 89.82, avg_delay_minutes: -11.28 },
  { origin: "BTM", destination: "SLC", carrier_code: "OO", carrier_name: "SkyWest Airlines", total_flights: 690, on_time_flights: 611, on_time_pct: 88.55, avg_delay_minutes: -14.18 },
  { origin: "PIH", destination: "SLC", carrier_code: "OO", carrier_name: "SkyWest Airlines", total_flights: 700, on_time_flights: 616, on_time_pct: 88.00, avg_delay_minutes: -9.15 },
  { origin: "SUN", destination: "SLC", carrier_code: "OO", carrier_name: "SkyWest Airlines", total_flights: 650, on_time_flights: 571, on_time_pct: 87.85, avg_delay_minutes: -8.45 },
  { origin: "GEG", destination: "SEA", carrier_code: "AS", carrier_name: "Alaska Airlines", total_flights: 1850, on_time_flights: 1624, on_time_pct: 87.78, avg_delay_minutes: -10.32 },
  { origin: "RDM", destination: "SEA", carrier_code: "AS", carrier_name: "Alaska Airlines", total_flights: 920, on_time_flights: 806, on_time_pct: 87.61, avg_delay_minutes: -12.67 },
  { origin: "PSC", destination: "SEA", carrier_code: "AS", carrier_name: "Alaska Airlines", total_flights: 780, on_time_flights: 682, on_time_pct: 87.44, avg_delay_minutes: -9.88 },
  { origin: "BIL", destination: "SLC", carrier_code: "OO", carrier_name: "SkyWest Airlines", total_flights: 1120, on_time_flights: 976, on_time_pct: 87.14, avg_delay_minutes: -7.23 },
  { origin: "JAC", destination: "SLC", carrier_code: "OO", carrier_name: "SkyWest Airlines", total_flights: 890, on_time_flights: 774, on_time_pct: 86.97, avg_delay_minutes: -6.54 },
  { origin: "MSO", destination: "SLC", carrier_code: "OO", carrier_name: "SkyWest Airlines", total_flights: 1050, on_time_flights: 912, on_time_pct: 86.86, avg_delay_minutes: -8.91 },
  { origin: "BOI", destination: "SLC", carrier_code: "OO", carrier_name: "SkyWest Airlines", total_flights: 2340, on_time_flights: 2025, on_time_pct: 86.54, avg_delay_minutes: -5.12 },
  { origin: "PDX", destination: "SEA", carrier_code: "AS", carrier_name: "Alaska Airlines", total_flights: 3450, on_time_flights: 2978, on_time_pct: 86.32, avg_delay_minutes: -4.78 },
  { origin: "SLC", destination: "BOI", carrier_code: "OO", carrier_name: "SkyWest Airlines", total_flights: 2290, on_time_flights: 1972, on_time_pct: 86.11, avg_delay_minutes: -6.34 },
  { origin: "FAT", destination: "SFO", carrier_code: "UA", carrier_name: "United Airlines", total_flights: 1560, on_time_flights: 1338, on_time_pct: 85.77, avg_delay_minutes: -3.21 },
  { origin: "MKE", destination: "DTW", carrier_code: "DL", carrier_name: "Delta Air Lines", total_flights: 2180, on_time_flights: 1864, on_time_pct: 85.50, avg_delay_minutes: -2.45 },
  { origin: "RNO", destination: "SFO", carrier_code: "UA", carrier_name: "United Airlines", total_flights: 1890, on_time_flights: 1612, on_time_pct: 85.29, avg_delay_minutes: -1.89 },
  { origin: "ABQ", destination: "DEN", carrier_code: "UA", carrier_name: "United Airlines", total_flights: 3210, on_time_flights: 2731, on_time_pct: 85.08, avg_delay_minutes: -2.67 },
  { origin: "DSM", destination: "ORD", carrier_code: "AA", carrier_name: "American Airlines", total_flights: 1680, on_time_flights: 1425, on_time_pct: 84.82, avg_delay_minutes: 0.12 },
  { origin: "CID", destination: "ORD", carrier_code: "AA", carrier_name: "American Airlines", total_flights: 1450, on_time_flights: 1228, on_time_pct: 84.69, avg_delay_minutes: 0.45 },
  { origin: "MSN", destination: "ORD", carrier_code: "AA", carrier_name: "American Airlines", total_flights: 1780, on_time_flights: 1505, on_time_pct: 84.55, avg_delay_minutes: 0.89 }
];

// ============================================
// QUERY 2: Delay Cause Breakdown by Carrier
// Fields: carrier_code, carrier_name, total_delayed_flights, avg_total_delay, avg_carrier_delay, avg_weather_delay, avg_nas_delay, avg_security_delay, avg_late_aircraft_delay
// ============================================
export const MOCK_DELAY_BREAKDOWN = [
  { carrier_code: "WN", carrier_name: "Southwest Airlines", total_delayed_flights: 278523, avg_total_delay: 55.48, avg_carrier_delay: 15.00, avg_weather_delay: 1.00, avg_nas_delay: 9.00, avg_security_delay: 0.00, avg_late_aircraft_delay: 28.00 },
  { carrier_code: "AA", carrier_name: "American Airlines", total_delayed_flights: 245502, avg_total_delay: 87.73, avg_carrier_delay: 28.00, avg_weather_delay: 4.00, avg_nas_delay: 11.00, avg_security_delay: 0.00, avg_late_aircraft_delay: 42.00 },
  { carrier_code: "DL", carrier_name: "Delta Air Lines", total_delayed_flights: 166417, avg_total_delay: 73.96, avg_carrier_delay: 35.00, avg_weather_delay: 3.00, avg_nas_delay: 13.00, avg_security_delay: 0.00, avg_late_aircraft_delay: 21.00 },
  { carrier_code: "UA", carrier_name: "United Airlines", total_delayed_flights: 142356, avg_total_delay: 78.45, avg_carrier_delay: 24.00, avg_weather_delay: 2.00, avg_nas_delay: 10.00, avg_security_delay: 0.00, avg_late_aircraft_delay: 40.00 },
  { carrier_code: "B6", carrier_name: "JetBlue Airways", total_delayed_flights: 67834, avg_total_delay: 92.15, avg_carrier_delay: 31.00, avg_weather_delay: 5.00, avg_nas_delay: 14.00, avg_security_delay: 1.00, avg_late_aircraft_delay: 39.00 },
  { carrier_code: "AS", carrier_name: "Alaska Airlines", total_delayed_flights: 49123, avg_total_delay: 64.20, avg_carrier_delay: 18.00, avg_weather_delay: 3.00, avg_nas_delay: 8.00, avg_security_delay: 0.00, avg_late_aircraft_delay: 33.00 },
  { carrier_code: "NK", carrier_name: "Spirit Airlines", total_delayed_flights: 41567, avg_total_delay: 102.30, avg_carrier_delay: 38.00, avg_weather_delay: 2.00, avg_nas_delay: 12.00, avg_security_delay: 1.00, avg_late_aircraft_delay: 47.00 },
  { carrier_code: "F9", carrier_name: "Frontier Airlines", total_delayed_flights: 28934, avg_total_delay: 96.80, avg_carrier_delay: 34.00, avg_weather_delay: 3.00, avg_nas_delay: 11.00, avg_security_delay: 0.00, avg_late_aircraft_delay: 46.00 },
  { carrier_code: "HA", carrier_name: "Hawaiian Airlines", total_delayed_flights: 12456, avg_total_delay: 71.50, avg_carrier_delay: 22.00, avg_weather_delay: 6.00, avg_nas_delay: 7.00, avg_security_delay: 0.00, avg_late_aircraft_delay: 35.00 },
  { carrier_code: "G4", carrier_name: "Allegiant Air", total_delayed_flights: 9823, avg_total_delay: 108.45, avg_carrier_delay: 41.00, avg_weather_delay: 2.00, avg_nas_delay: 10.00, avg_security_delay: 1.00, avg_late_aircraft_delay: 52.00 }
];

// ============================================
// QUERY 3: Airports with Most Departure Delays (TOP 25)
// Fields: airport_code, total_flights, delayed_departures, delay_rate_pct, avg_departure_delay, avg_delay_when_delayed, max_departure_delay
// ============================================
export const MOCK_AIRPORT_DELAYS = [
  { airport_code: "DFW", total_flights: 307657, delayed_departures: 82262, delay_rate_pct: 26.74, avg_departure_delay: 6.85, avg_delay_when_delayed: 49.04, max_departure_delay: 2111 },
  { airport_code: "DEN", total_flights: 305048, delayed_departures: 68217, delay_rate_pct: 22.36, avg_departure_delay: 4.23, avg_delay_when_delayed: 35.46, max_departure_delay: 2028 },
  { airport_code: "ATL", total_flights: 338428, delayed_departures: 64580, delay_rate_pct: 19.08, avg_departure_delay: 3.98, avg_delay_when_delayed: 38.36, max_departure_delay: 2353 },
  { airport_code: "ORD", total_flights: 298765, delayed_departures: 61234, delay_rate_pct: 20.50, avg_departure_delay: 4.67, avg_delay_when_delayed: 42.18, max_departure_delay: 1987 },
  { airport_code: "LAX", total_flights: 287934, delayed_departures: 54821, delay_rate_pct: 19.04, avg_departure_delay: 3.82, avg_delay_when_delayed: 36.92, max_departure_delay: 1856 },
  { airport_code: "PHX", total_flights: 265432, delayed_departures: 52617, delay_rate_pct: 19.82, avg_departure_delay: 3.56, avg_delay_when_delayed: 33.57, max_departure_delay: 1734 },
  { airport_code: "LAS", total_flights: 248976, delayed_departures: 48923, delay_rate_pct: 19.65, avg_departure_delay: 3.34, avg_delay_when_delayed: 31.45, max_departure_delay: 1612 },
  { airport_code: "IAH", total_flights: 234567, delayed_departures: 46312, delay_rate_pct: 19.74, avg_departure_delay: 4.28, avg_delay_when_delayed: 40.28, max_departure_delay: 1923 },
  { airport_code: "SFO", total_flights: 223456, delayed_departures: 44876, delay_rate_pct: 20.08, avg_departure_delay: 4.89, avg_delay_when_delayed: 45.12, max_departure_delay: 2145 },
  { airport_code: "EWR", total_flights: 198765, delayed_departures: 41234, delay_rate_pct: 20.75, avg_departure_delay: 5.45, avg_delay_when_delayed: 48.67, max_departure_delay: 2234 },
  { airport_code: "CLT", total_flights: 212345, delayed_departures: 38954, delay_rate_pct: 18.35, avg_departure_delay: 3.45, avg_delay_when_delayed: 34.89, max_departure_delay: 1567 },
  { airport_code: "MCO", total_flights: 203456, delayed_departures: 36782, delay_rate_pct: 18.08, avg_departure_delay: 3.18, avg_delay_when_delayed: 32.45, max_departure_delay: 1445 },
  { airport_code: "SEA", total_flights: 189234, delayed_departures: 34512, delay_rate_pct: 18.24, avg_departure_delay: 3.34, avg_delay_when_delayed: 33.78, max_departure_delay: 1521 },
  { airport_code: "MIA", total_flights: 176543, delayed_departures: 32987, delay_rate_pct: 18.68, avg_departure_delay: 3.82, avg_delay_when_delayed: 37.92, max_departure_delay: 1689 },
  { airport_code: "JFK", total_flights: 198234, delayed_departures: 31456, delay_rate_pct: 15.87, avg_departure_delay: 3.56, avg_delay_when_delayed: 41.23, max_departure_delay: 1834 },
  { airport_code: "BOS", total_flights: 156789, delayed_departures: 29234, delay_rate_pct: 18.64, avg_departure_delay: 3.99, avg_delay_when_delayed: 39.56, max_departure_delay: 1712 },
  { airport_code: "DTW", total_flights: 147856, delayed_departures: 27645, delay_rate_pct: 18.70, avg_departure_delay: 3.67, avg_delay_when_delayed: 36.34, max_departure_delay: 1598 },
  { airport_code: "MSP", total_flights: 139876, delayed_departures: 25987, delay_rate_pct: 18.57, avg_departure_delay: 3.42, avg_delay_when_delayed: 34.12, max_departure_delay: 1476 },
  { airport_code: "LGA", total_flights: 132456, delayed_departures: 24823, delay_rate_pct: 18.74, avg_departure_delay: 4.45, avg_delay_when_delayed: 43.89, max_departure_delay: 1945 },
  { airport_code: "FLL", total_flights: 128934, delayed_departures: 23456, delay_rate_pct: 18.19, avg_departure_delay: 3.50, avg_delay_when_delayed: 35.67, max_departure_delay: 1534 },
  { airport_code: "SLC", total_flights: 145678, delayed_departures: 22134, delay_rate_pct: 15.19, avg_departure_delay: 2.42, avg_delay_when_delayed: 29.45, max_departure_delay: 1312 },
  { airport_code: "BWI", total_flights: 118765, delayed_departures: 21345, delay_rate_pct: 17.97, avg_departure_delay: 3.56, avg_delay_when_delayed: 36.78, max_departure_delay: 1623 },
  { airport_code: "DCA", total_flights: 112389, delayed_departures: 20234, delay_rate_pct: 18.00, avg_departure_delay: 3.78, avg_delay_when_delayed: 38.92, max_departure_delay: 1734 },
  { airport_code: "PHL", total_flights: 107654, delayed_departures: 19456, delay_rate_pct: 18.07, avg_departure_delay: 3.92, avg_delay_when_delayed: 40.12, max_departure_delay: 1812 },
  { airport_code: "SAN", total_flights: 109823, delayed_departures: 18923, delay_rate_pct: 17.23, avg_departure_delay: 3.01, avg_delay_when_delayed: 32.56, max_departure_delay: 1423 }
];

// ============================================
// QUERY 4: Complete Carrier Performance Scorecard
// Fields: carrier_code, carrier_name, total_flights, delayed_flights, delay_rate_pct, avg_arrival_delay, avg_departure_delay, avg_carrier_delay, avg_weather_delay, avg_nas_delay
// ============================================
export const MOCK_CARRIER_SCORECARD = [
  { carrier_code: "WN", carrier_name: "Southwest Airlines", total_flights: 1404597, delayed_flights: 278523, delay_rate_pct: 19.83, avg_arrival_delay: 5.13, avg_departure_delay: 11.71, avg_carrier_delay: 3.00, avg_weather_delay: 0.00, avg_nas_delay: 1.00 },
  { carrier_code: "DL", carrier_name: "Delta Air Lines", total_flights: 997846, delayed_flights: 166417, delay_rate_pct: 16.68, avg_arrival_delay: 3.66, avg_departure_delay: 10.11, avg_carrier_delay: 5.00, avg_weather_delay: 0.00, avg_nas_delay: 2.00 },
  { carrier_code: "AA", carrier_name: "American Airlines", total_flights: 966116, delayed_flights: 245502, delay_rate_pct: 25.41, avg_arrival_delay: 15.31, avg_departure_delay: 20.58, avg_carrier_delay: 7.00, avg_weather_delay: 1.00, avg_nas_delay: 2.00 },
  { carrier_code: "UA", carrier_name: "United Airlines", total_flights: 678234, delayed_flights: 142356, delay_rate_pct: 20.99, avg_arrival_delay: 7.45, avg_departure_delay: 13.23, avg_carrier_delay: 5.00, avg_weather_delay: 0.00, avg_nas_delay: 2.00 },
  { carrier_code: "B6", carrier_name: "JetBlue Airways", total_flights: 334567, delayed_flights: 67834, delay_rate_pct: 20.28, avg_arrival_delay: 8.92, avg_departure_delay: 15.67, avg_carrier_delay: 6.00, avg_weather_delay: 1.00, avg_nas_delay: 2.00 },
  { carrier_code: "AS", carrier_name: "Alaska Airlines", total_flights: 243891, delayed_flights: 49123, delay_rate_pct: 20.14, avg_arrival_delay: 4.23, avg_departure_delay: 9.78, avg_carrier_delay: 3.00, avg_weather_delay: 0.00, avg_nas_delay: 1.00 },
  { carrier_code: "NK", carrier_name: "Spirit Airlines", total_flights: 178934, delayed_flights: 41567, delay_rate_pct: 23.23, avg_arrival_delay: 12.67, avg_departure_delay: 18.45, avg_carrier_delay: 8.00, avg_weather_delay: 0.00, avg_nas_delay: 2.00 },
  { carrier_code: "F9", carrier_name: "Frontier Airlines", total_flights: 123456, delayed_flights: 28934, delay_rate_pct: 23.44, avg_arrival_delay: 11.89, avg_departure_delay: 17.23, avg_carrier_delay: 7.00, avg_weather_delay: 0.00, avg_nas_delay: 2.00 },
  { carrier_code: "HA", carrier_name: "Hawaiian Airlines", total_flights: 67823, delayed_flights: 12456, delay_rate_pct: 18.36, avg_arrival_delay: 6.34, avg_departure_delay: 11.92, avg_carrier_delay: 4.00, avg_weather_delay: 1.00, avg_nas_delay: 1.00 },
  { carrier_code: "G4", carrier_name: "Allegiant Air", total_flights: 45678, delayed_flights: 9823, delay_rate_pct: 21.51, avg_arrival_delay: 13.45, avg_departure_delay: 19.67, avg_carrier_delay: 8.00, avg_weather_delay: 0.00, avg_nas_delay: 2.00 }
];

// ============================================
// UTILITY FUNCTIONS
// ============================================

export const formatNumber = (num) => {
  return num.toLocaleString('en-US');
};

export const formatPercent = (pct) => {
  return `${pct}%`;
};

export const formatCurrency = (amount) => {
  return `$${(amount / 1000000).toFixed(0)}M`;
};

export const formatDelay = (minutes) => {
  if (minutes < 0) return `${Math.abs(minutes).toFixed(1)} min early`;
  return `${minutes.toFixed(1)} min delay`;
};
