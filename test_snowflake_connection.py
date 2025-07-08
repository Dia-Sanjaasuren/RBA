import os
# Disable OCSP checks to avoid certificate validation errors
os.environ['SF_OCSP_RESPONSE_CACHE_DIR'] = ''
os.environ['SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED'] = 'false'
os.environ['SF_OCSP_ACTIVATE_ENDPOINT'] = 'false'
os.environ['SF_OCSP_FAIL_OPEN'] = 'true'
os.environ['SF_OCSP_TESTING_MODE'] = 'true'
os.environ['SNOWFLAKE_OCSP_RESPONSE_CACHE_SERVER_ENABLED'] = 'false'

import snowflake.connector
from config import SNOWFLAKE_CONFIG

def test_connection():
    try:
        # Add OCSP settings to connection config
        connection_config = SNOWFLAKE_CONFIG.copy()
        connection_config.update({
            'ocsp_response_cache_dir': '',
            'ocsp_response_cache_server_enabled': False,
            'ocsp_activate_endpoint': False,
            'ocsp_fail_open': True,
            'ocsp_testing_mode': True
        })
        
        print("Attempting to connect to Snowflake...")
        conn = snowflake.connector.connect(**connection_config)
        
        # Test a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        result = cursor.fetchone()
        print(f"Connection successful! Snowflake version: {result[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Connection failed: {str(e)}")
        return False

if __name__ == "__main__":
    test_connection() 