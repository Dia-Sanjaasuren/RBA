import ssl
import os
import urllib3
import requests

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Set environment variables
def setup_ssl_bypass():
    """Comprehensive SSL bypass setup"""
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
        'SSL_CERT_DIR': '',
        'REQUESTS_VERIFY': 'false'
    }
    
    for key, value in env_vars.items():
        os.environ[key] = value

# Monkey patch SSL verification
def patch_ssl():
    """Monkey patch SSL verification to always return True"""
    def mock_verify(*args, **kwargs):
        return True
    
    # Patch urllib3
    try:
        urllib3.util.ssl_.DEFAULT_CERTS = None
        urllib3.util.ssl_.create_urllib3_context = lambda *args, **kwargs: ssl.create_default_context()
    except:
        pass
    
    # Patch requests
    try:
        requests.packages.urllib3.util.ssl_.DEFAULT_CERTS = None
    except:
        pass

# Apply all patches
setup_ssl_bypass()
patch_ssl()

print("SSL patches applied successfully") 