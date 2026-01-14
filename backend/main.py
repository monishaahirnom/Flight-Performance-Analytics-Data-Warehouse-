from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pyodbc
import time
import re
import os

app = FastAPI(title="Flight Data Warehouse API", version="1.0.0")

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
SQL_SERVER = os.getenv("SQL_SERVER", "localhost\\SQLEXPRESS")
DATABASE_NAME = "FlightDataWarehouse"
NORMALIZED_DATABASE_NAME = "flight_analytics"

CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={DATABASE_NAME};"
    "Trusted_Connection=yes;"
)

NORMALIZED_CONNECTION_STRING = (
    f"DRIVER={{ODBC Driver 17 for SQL Server}};"
    f"SERVER={SQL_SERVER};"
    f"DATABASE={NORMALIZED_DATABASE_NAME};"
    "Trusted_Connection=yes;"
)

class QueryRequest(BaseModel):
    query: str

def get_connection():
    """Get connection to Data Warehouse"""
    return pyodbc.connect(CONNECTION_STRING)

def get_normalized_connection():
    """Get connection to Normalized Database"""
    return pyodbc.connect(NORMALIZED_CONNECTION_STRING)

# Predefined queries
PREDEFINED_QUERIES = {
    "query1": {
        "name": "Best Carriers by Route (On-Time Performance)",
        "description": "Which carriers have the best on-time performance by route?",
        "sql": """SELECT TOP 20
        orig.airport_code AS origin,
        dest_apt.airport_code AS destination,
        a.carrier_code,
        a.carrier_name,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN d.arrival_delay <= 0 THEN 1 ELSE 0 END) AS on_time_flights,
        CAST(ROUND(100.0 * SUM(CASE WHEN d.arrival_delay <= 0 THEN 1 ELSE 0 END) / COUNT(*), 2) AS DECIMAL(5,2)) AS on_time_pct,
        CAST(ROUND(AVG(d.arrival_delay), 2) AS DECIMAL(10,2)) AS avg_delay_minutes
    FROM dbo.Fact_Delays d
    INNER JOIN dbo.Dim_Airport orig ON d.origin_airport_key = orig.airport_key
    INNER JOIN dbo.Dim_Airport dest_apt ON d.dest_airport_key = dest_apt.airport_key
    INNER JOIN dbo.Dim_Airline a ON d.airline_key = a.airline_key
    WHERE d.arrival_delay IS NOT NULL
    GROUP BY orig.airport_code, dest_apt.airport_code, a.carrier_code, a.carrier_name
    HAVING COUNT(*) >= 500
    ORDER BY on_time_pct DESC, total_flights DESC"""
    },

    "query2": {
        "name": "Delay Cause Breakdown by Carrier",
        "description": "Root cause analysis of delays by carrier.",
        "sql": """SELECT 
        a.carrier_code,
        a.carrier_name,
        COUNT(*) AS total_delayed_flights,
        CAST(ROUND(AVG(d.arrival_delay), 2) AS DECIMAL(10,2)) AS avg_total_delay,
        CAST(ROUND(AVG(d.carrier_delay), 2) AS DECIMAL(10,2)) AS avg_carrier_delay,
        CAST(ROUND(AVG(d.weather_delay), 2) AS DECIMAL(10,2)) AS avg_weather_delay,
        CAST(ROUND(AVG(d.nas_delay), 2) AS DECIMAL(10,2)) AS avg_nas_delay,
        CAST(ROUND(AVG(d.security_delay), 2) AS DECIMAL(10,2)) AS avg_security_delay,
        CAST(ROUND(AVG(d.late_aircraft_delay), 2) AS DECIMAL(10,2)) AS avg_late_aircraft_delay
    FROM dbo.Fact_Delays d
    INNER JOIN dbo.Dim_Airline a ON d.airline_key = a.airline_key
    WHERE d.is_delayed = 1 AND d.arrival_delay > 0
    GROUP BY a.carrier_code, a.carrier_name
    ORDER BY total_delayed_flights DESC"""
    },

    "query3": {
        "name": "Airports with Most Departure Delays",
        "description": "Which airports need more resources?",
        "sql": """SELECT TOP 25
        apt.airport_code,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN d.departure_delay > 15 THEN 1 ELSE 0 END) AS delayed_departures,
        CAST(ROUND(100.0 * SUM(CASE WHEN d.departure_delay > 15 THEN 1 ELSE 0 END) / COUNT(*), 2) AS DECIMAL(5,2)) AS delay_rate_pct,
        CAST(ROUND(AVG(d.departure_delay), 2) AS DECIMAL(10,2)) AS avg_departure_delay,
        CAST(ROUND(AVG(CASE WHEN d.departure_delay > 15 THEN d.departure_delay END), 2) AS DECIMAL(10,2)) AS avg_delay_when_delayed,
        CAST(MAX(d.departure_delay) AS INT) AS max_departure_delay
    FROM dbo.Fact_Delays d
    INNER JOIN dbo.Dim_Airport apt ON d.origin_airport_key = apt.airport_key
    WHERE d.departure_delay IS NOT NULL
    GROUP BY apt.airport_code
    HAVING COUNT(*) >= 1000
    ORDER BY delayed_departures DESC"""
    },

    "query4": {
        "name": "Complete Carrier Performance Scorecard",
        "description": "Comprehensive metrics (30+ sec on normalized DB).",
        "sql": """SELECT 
        a.carrier_code,
        a.carrier_name,
        COUNT(*) AS total_flights,
        SUM(CASE WHEN d.is_delayed = 1 THEN 1 ELSE 0 END) AS delayed_flights,
        CAST(ROUND(100.0 * SUM(CASE WHEN d.is_delayed = 1 THEN 1 ELSE 0 END) / COUNT(*), 2) AS DECIMAL(5,2)) AS delay_rate_pct,
        CAST(ROUND(AVG(d.arrival_delay), 2) AS DECIMAL(10,2)) AS avg_arrival_delay,
        CAST(ROUND(AVG(d.departure_delay), 2) AS DECIMAL(10,2)) AS avg_departure_delay,
        CAST(ROUND(AVG(d.carrier_delay), 2) AS DECIMAL(10,2)) AS avg_carrier_delay,
        CAST(ROUND(AVG(d.weather_delay), 2) AS DECIMAL(10,2)) AS avg_weather_delay,
        CAST(ROUND(AVG(d.nas_delay), 2) AS DECIMAL(10,2)) AS avg_nas_delay
    FROM dbo.Fact_Delays d
    INNER JOIN dbo.Dim_Airline a ON d.airline_key = a.airline_key
    WHERE d.arrival_delay IS NOT NULL
    GROUP BY a.carrier_code, a.carrier_name
    ORDER BY total_flights DESC"""
    }
}

def convert_to_normalized_query(warehouse_query: str) -> str:
    """Convert warehouse query to normalized Q1-Q4 structure"""
    query = warehouse_query
    
    # Replace Fact_Delays with UNION ALL
    query = re.sub(
        r'FROM\s+(dbo\.)?Fact_Delays\s+d',
        '''FROM (
    SELECT * FROM Q1 WHERE cancelled = 0
    UNION ALL
    SELECT * FROM Q2 WHERE cancelled = 0
    UNION ALL
    SELECT * FROM Q3 WHERE cancelled = 0
    UNION ALL
    SELECT * FROM Q4 WHERE cancelled = 0
) d''',
        query,
        flags=re.IGNORECASE
    )
    
    # Remove dimension joins
    query = re.sub(r'\s*INNER\s+JOIN\s+dbo\.Dim_Airport\s+(orig|dest_apt|apt)\s+ON\s+[^\n]+', '', query, flags=re.IGNORECASE)
    query = re.sub(r'\s*INNER\s+JOIN\s+dbo\.Dim_Airline\s+a\s+ON\s+[^\n]+', '', query, flags=re.IGNORECASE)
    
    # Replace column names
    query = query.replace("d.arrival_delay", "d.arr_delay")
    query = query.replace("d.departure_delay", "d.dep_delay")
    query = query.replace("d.is_delayed", "CASE WHEN d.arr_delay > 0 THEN 1 ELSE 0 END")
    query = query.replace("orig.airport_code", "d.origin")
    query = query.replace("dest_apt.airport_code", "d.dest")
    query = query.replace("apt.airport_code", "d.origin")
    
    # Handle carrier columns
    query = re.sub(r'\ba\.carrier_code\b', 'd.op_unique_carrier AS carrier_code', query, flags=re.IGNORECASE)
    query = re.sub(r'\ba\.carrier_name\b', 'd.op_unique_carrier AS carrier_name', query, flags=re.IGNORECASE)
    
    # Fix GROUP BY - remove AS aliases
    def fix_group_by(match):
        clause = match.group(0)
        clause = re.sub(r'\s+AS\s+carrier_code', '', clause, flags=re.IGNORECASE)
        clause = re.sub(r'\s+AS\s+carrier_name', '', clause, flags=re.IGNORECASE)
        parts = clause.split('GROUP BY', 1)
        if len(parts) == 2:
            cols = [c.strip() for c in parts[1].split(',')]
            unique = []
            seen = set()
            for col in cols:
                norm = col.lower().replace(' ', '')
                if norm not in seen:
                    seen.add(norm)
                    unique.append(col)
            clause = 'GROUP BY ' + ', '.join(unique)
        return clause
    
    query = re.sub(r'GROUP\s+BY\s+[^\n]+(?:\n\s*[^\n]+)*?(?=\s*(?:HAVING|ORDER|$))', fix_group_by, query, flags=re.IGNORECASE | re.DOTALL)
    
    return query

@app.get("/")
async def root():
    return {"message": "Flight Data Warehouse API", "version": "1.0.0", "status": "running"}

@app.post("/api/query/execute")
async def execute_query(request: QueryRequest):
    """Execute query on warehouse only"""
    try:
        start = time.time()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(request.query)
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        exec_time = (time.time() - start) * 1000
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "data": results,
            "execution_time_ms": round(exec_time, 2),
            "row_count": len(results),
            "columns": columns
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/query/warehouse")
async def execute_warehouse_query(request: QueryRequest):
    """Execute query on warehouse database only"""
    try:
        start = time.time()
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute(request.query)
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        exec_time = (time.time() - start) * 1000
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "data": results,
            "execution_time_ms": round(exec_time, 2),
            "row_count": len(results),
            "columns": columns
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/query/normalized")
async def execute_normalized_query(request: QueryRequest):
    """Execute query on normalized database only"""
    try:
        start = time.time()
        conn = get_normalized_connection()
        cursor = conn.cursor()
        n_query = convert_to_normalized_query(request.query)
        print(f"DEBUG - Converted query: {n_query}")
        cursor.execute(n_query)
        columns = [col[0] for col in cursor.description]
        results = [dict(zip(columns, row)) for row in cursor.fetchall()]
        exec_time = (time.time() - start) * 1000
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "data": results,
            "execution_time_ms": round(exec_time, 2),
            "row_count": len(results),
            "columns": columns
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/query/compare")
async def compare_databases(request: QueryRequest):
    """Execute query on BOTH databases and compare performance"""
    try:
        # Warehouse execution
        w_start = time.time()
        w_conn = get_connection()
        w_cursor = w_conn.cursor()
        w_cursor.execute(request.query)
        w_cols = [col[0] for col in w_cursor.description]
        w_results = [dict(zip(w_cols, row)) for row in w_cursor.fetchall()]
        w_time = (time.time() - w_start) * 1000
        w_cursor.close()
        w_conn.close()
        
        # Normalized execution
        n_start = time.time()
        n_conn = get_normalized_connection()
        n_cursor = n_conn.cursor()
        n_query = convert_to_normalized_query(request.query)
        print(f"DEBUG - Converted query: {n_query}")
        n_cursor.execute(n_query)
        n_cols = [col[0] for col in n_cursor.description]
        n_results = [dict(zip(n_cols, row)) for row in n_cursor.fetchall()]
        n_time = (time.time() - n_start) * 1000
        n_cursor.close()
        n_conn.close()
        
        speedup = n_time / w_time if w_time > 0 else 1.0
        improvement = ((n_time - w_time) / n_time) * 100 if n_time > 0 else 0.0
        
        return {
            "success": True,
            "warehouse": {
                "data": w_results,
                "execution_time_ms": round(w_time, 2),
                "row_count": len(w_results),
                "columns": w_cols
            },
            "normalized": {
                "data": n_results,
                "execution_time_ms": round(n_time, 2),
                "row_count": len(n_results),
                "columns": n_cols
            },
            "comparison": {
                "speedup": round(speedup, 2),
                "improvement_pct": round(improvement, 1),
                "time_saved_ms": round(n_time - w_time, 2)
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Comparison failed: {str(e)}")

@app.get("/api/query/predefined")
async def get_predefined_queries():
    """Get all predefined queries"""
    return {"queries": list(PREDEFINED_QUERIES.values())}

@app.get("/api/metrics/database")
async def get_database_metrics():
    """Get database statistics"""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM Fact_FlightPerformance) as total_flights,
                (SELECT COUNT(*) FROM Dim_Airport) as total_airports,
                (SELECT CAST(ROUND(AVG(CAST(arrival_delay AS FLOAT)), 2) AS DECIMAL(10,2)) 
                 FROM Fact_Delays WHERE arrival_delay IS NOT NULL) as avg_delay
        """)
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        
        return {
            "success": True,
            "metrics": {
                "total_flights": row[0],
                "total_airports": row[1],
                "avg_delay_minutes": float(row[2]) if row[2] else 0.0
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)