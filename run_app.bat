@echo off
REM Set Snowflake environment variables to disable OCSP checks
set SF_OCSP_RESPONSE_CACHE_DIR=
set SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED=false
set SF_OCSP_ACTIVATE_ENDPOINT=false
set SF_OCSP_FAIL_OPEN=true
set SF_OCSP_TESTING_MODE=true
set SNOWFLAKE_OCSP_RESPONSE_CACHE_SERVER_ENABLED=false
set SF_SSL_VERIFY=false
set SF_SSL_CERT_REQS=CERT_NONE

REM Run the Streamlit app
python -m streamlit run home_2.py 