from config import SNOWFLAKE_CONFIG
import snowflake.connector

def test_connection():
    try:
        print("Attempting to connect to Snowflake...")
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        
        # Test a simple query
        cur = conn.cursor()
        cur.execute('SELECT CURRENT_VERSION()')
        version = cur.fetchone()[0]
        print(f"Successfully connected to Snowflake!")
        print(f"Snowflake version: {version}")
        
        cur.close()
        conn.close()
        print("Connection test completed successfully!")
        
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")

if __name__ == "__main__":
    test_connection() 