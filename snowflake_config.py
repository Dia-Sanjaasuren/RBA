# Global Snowflake configuration - must be imported first
import os

# Disable OCSP checks to avoid certificate validation errors
# These must be set before any Snowflake imports
os.environ['SF_OCSP_RESPONSE_CACHE_DIR'] = ''
os.environ['SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED'] = 'false'
os.environ['SF_OCSP_ACTIVATE_ENDPOINT'] = 'false'
os.environ['SF_OCSP_FAIL_OPEN'] = 'true'
os.environ['SF_OCSP_TESTING_MODE'] = 'true'
os.environ['SNOWFLAKE_OCSP_RESPONSE_CACHE_SERVER_ENABLED'] = 'false'

# Additional SSL settings that might help
os.environ['SF_SSL_VERIFY'] = 'false'
os.environ['SF_SSL_CERT_REQS'] = 'CERT_NONE'

print("Snowflake environment variables set globally") 