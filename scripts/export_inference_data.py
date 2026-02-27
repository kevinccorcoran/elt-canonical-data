import os
import json
import psycopg2
from decimal import Decimal

def get_connection():
    # Attempt to get connection details from environment
    env = os.getenv("ENV", "dev")
    
    # Try to find a valid connection string
    # We'll check DATABASE_URL first, then look for ENV-specific ones
    db_url = os.getenv("DATABASE_URL")
    
    if not db_url:
        # Fallback to building it from parts if available
        user = os.getenv("DB_USER")
        password = os.getenv("DB_PASSWORD")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        dbname = "prod" if env == "prod" else os.getenv("DB_DATABASE", "dev")
        ssl = "require" if env == "prod" else "prefer"
        
        if user and password:
            db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}?sslmode={ssl}"
    
    if not db_url:
        raise RuntimeError("Could not find database connection configuration (DATABASE_URL or DB_ vars)")
    
    # DO specific check if needed
    if "defaultdb" in db_url.lower() or "prod" in db_url.lower():
        print(f"Connecting to: {db_url.split('@')[1]}")
    
    return psycopg2.connect(db_url)

def export_data():
    query = """
    SELECT 
        past_excess_return_z_bucket_num as bucket,
        record_pct_per_id as pct,
        median_future_excess_return_vs_spy as returns,
        p05_past_excess_return_vs_spy as past_p05,
        p95_past_excess_return_vs_spy as past_p95,
        p05_future_excess_return_vs_spy as future_p05,
        p95_future_excess_return_vs_spy as future_p95
    FROM inference.return_expectation_decomposition
    WHERE fibonacci_lag_value = 4 
      AND future_fibonacci_lag_value = 7
      AND id = 4
    ORDER BY past_excess_return_z_bucket_num;
    """
    
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            
            data = []
            for row in rows:
                item = dict(zip(colnames, row))
                # Convert Decimals to floats for JSON serialization
                for k, v in item.items():
                    if isinstance(v, Decimal):
                        item[k] = float(v)
                data.append(item)
            
            # Save to JSON
            output_path = os.path.join(os.path.dirname(__file__), "data.json")
            with open(output_path, "w") as f:
                json.dump(data, f, indent=4)
            
            print(f"Successfully exported {len(data)} rows to {output_path}")
            
    except Exception as e:
        print(f"Error exporting data: {e}")

if __name__ == "__main__":
    export_data()
