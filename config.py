from dotenv import load_dotenv
import os
import snowflake.connector

# Load environment variables from .env file
load_dotenv()

# Snowflake connection parameters
SNOWFLAKE_CONFIG = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "database": os.getenv("SNOWFLAKE_DATABASE"),
    "schema": os.getenv("SNOWFLAKE_SCHEMA"),
    "insecure_mode": True  # Disables OCSP checks for local/dev
}

def get_snowflake_connection():
    """Create and return a new Snowflake connection using the config above."""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG) 