# Set Snowflake environment variables to disable OCSP checks
$env:SF_OCSP_RESPONSE_CACHE_DIR = ""
$env:SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED = "false"
$env:SF_OCSP_ACTIVATE_ENDPOINT = "false"
$env:SF_OCSP_FAIL_OPEN = "true"
$env:SF_OCSP_TESTING_MODE = "true"
$env:SNOWFLAKE_OCSP_RESPONSE_CACHE_SERVER_ENABLED = "false"
$env:SF_SSL_VERIFY = "false"
$env:SF_SSL_CERT_REQS = "CERT_NONE"
$env:PYTHONHTTPSVERIFY = "0"
$env:REQUESTS_VERIFY = "false"

Write-Host "Environment variables set. Starting Streamlit app..."

# Run the Streamlit app
python -m streamlit run home_2.py 