import ssl
import os

# Create a custom SSL context that bypasses certificate verification
def create_ssl_context():
    """Create an SSL context that bypasses certificate verification"""
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context

# Set environment variables for Snowflake
def setup_snowflake_environment():
    """Set all environment variables needed to bypass OCSP and SSL verification"""
    env_vars = {
        'SF_OCSP_RESPONSE_CACHE_DIR': '',
        'SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED': 'false',
        'SF_OCSP_ACTIVATE_ENDPOINT': 'false',
        'SF_OCSP_FAIL_OPEN': 'true',
        'SF_OCSP_TESTING_MODE': 'true',
        'SNOWFLAKE_OCSP_RESPONSE_CACHE_SERVER_ENABLED': 'false',
        'SF_SSL_VERIFY': 'false',
        'SF_SSL_CERT_REQS': 'CERT_NONE',
        'PYTHONHTTPSVERIFY': '0',
        'CURL_CA_BUNDLE': '',
        'REQUESTS_CA_BUNDLE': '',
        'SSL_CERT_FILE': '',
        'SSL_CERT_DIR': ''
    }
    
    for key, value in env_vars.items():
        os.environ[key] = value
    
    print("SSL bypass environment variables set")

# Call setup immediately when module is imported
setup_snowflake_environment() 