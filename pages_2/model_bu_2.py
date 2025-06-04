import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder
from config import SNOWFLAKE_CONFIG
import numpy as np

# Define payment method order for all tables
payment_method_order = [
    'AMEX',
    'EFTPOS',
    'Domestic Debit',
    'Domestic Credit',
    'Premium Debit',
    'Premium Credit',
    'Int.Debit',
    'Int.Credit'
]

# Define Wpay card type percentages
wpay_card_types = [
    ("AMEX", 0.0140),
    ("EFTPOS", 0.3208),
    ("Domestic Debit", 0.2500),
    ("Domestic Credit", 0.1850),
    ("Premium Debit", 0.0952),
    ("Premium Credit", 0.1000),
    ("Int.Debit", 0.0050),
    ("Int.Credit", 0.0300)
]

# Helper functions for formatting
def format_currency(value):
    if pd.isna(value) or value == 0:
        return "$0"
    return f"${value:,.0f}"

def format_percent(value):
    if pd.isna(value):
        return "0.00%"
    return f"{value:.2f}%"

def get_previous_month():
    today = datetime.today()
    first = today.replace(day=1)
    prev_month = first - timedelta(days=1)
    return prev_month.strftime('%Y-%m')

@st.cache_resource
def init_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            **SNOWFLAKE_CONFIG,
            client_session_keep_alive=True
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None

@st.cache_data
def get_filter_options():
    conn = init_snowflake_connection()
    df = pd.read_sql("SELECT DISTINCT SOURCE, DISPLAY_NAME, ACQUIRER, TRADING_MONTH FROM dia_db.public.rba_model_data", conn)
    business_units = sorted(df['SOURCE'].dropna().unique().tolist())
    acquirers = sorted(df['ACQUIRER'].dropna().unique().tolist())
    months = sorted(df['TRADING_MONTH'].dropna().unique().tolist())
    return business_units, acquirers, months

st.subheader("Business Unit Model Summary")

# Get filter options
business_units, acquirers, months = get_filter_options()
default_month = get_previous_month()

months_desc = sorted(months, reverse=True)
all_months = ["All"] + months_desc
all_bu = ["All"] + business_units
all_acquirer = ["All"] + acquirers

col1, col2, col3 = st.columns(3)
with col1:
    selected_bu = st.multiselect("Business Unit", all_bu, default=["All"], key="model_bu_2")
with col2:
    selected_acquirer = st.multiselect("Acquirer", all_acquirer, default=["All"], key="model_acquirer_2")
with col3:
    selected_months = st.multiselect("Trading Month", all_months, default=[default_month], key="model_month_2")

def get_selected_or_all(selected, all_values):
    if "All" in selected or not selected:
        return all_values
    return selected

bu_filter = get_selected_or_all(selected_bu, business_units)
acquirer_filter = get_selected_or_all(selected_acquirer, acquirers)
month_filter = get_selected_or_all(selected_months, months_desc)

def get_metric_data(bu_list, acquirer_list, month_list):
    conn = init_snowflake_connection()
    where_clauses = []
    if bu_list and len(bu_list) < len(business_units):
        bu_str = ", ".join([f"'{b}'" for b in bu_list])
        where_clauses.append(f"SOURCE IN ({bu_str})")
    if acquirer_list and len(acquirer_list) < len(acquirers):
        acq_str = ", ".join([f"'{a}'" for a in acquirer_list])
        where_clauses.append(f"ACQUIRER IN ({acq_str})")
    if month_list and len(month_list) < len(months_desc):
        month_str = ", ".join([f"'{m}'" for m in month_list])
        where_clauses.append(f"TRADING_MONTH IN ({month_str})")
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
    query = f'''
    SELECT 
        CASE 
            WHEN SOURCE = 'Swiftpos_Reseller' THEN 'SwiftPOS Reseller'
            WHEN SOURCE = 'OolioPay' THEN 'Oolio Pay'
            WHEN SOURCE = 'IdealPOS_Reseller' THEN 'IdealPOS Reseller'
            WHEN SOURCE IN ('Oolio', 'OolioPaymentPlatform') THEN 'Oolio Platform'
            ELSE SOURCE
        END AS "Business Unit",
        CASE 
            WHEN payment_method_variant IN ('amex','amex_applepay','amex_googlepay') THEN 'AMEX'
            WHEN payment_method_variant IN ('eftpos_australia','eftpos_australia_chq','eftpos_australia_sav') THEN 'EFTPOS'
            WHEN payment_method_variant IN ('visa','mcstandardcredit','mccredit','visastandardcredit','mc') THEN 'VC/MC Domestic Credit'
            WHEN payment_method_variant IN ('visaprepaidanonymous','visastandarddebit','mcprepaidanonymous','mcdebit','maestro','mcstandarddebit') THEN 'VC/MC Domestic Debit'
            WHEN payment_method_variant IN ('visapremiumcredit','mcpremiumcredit','visacorporatecredit','visacommercialpremiumcredit','mc_applepay','visacommercialsuperpremiumcredit','visa_applepay','mccorporatecredit','visa_googlepay','mccommercialcredit','mcfleetcredit','visabusiness','mcpurchasingcredit','visapurchasingcredit','mc_googlepay','visasuperpremiumcredit','visacommercialcredit','visafleetcredit','mcsuperpremiumcredit') THEN 'VC/MC Premium Credit'
            WHEN payment_method_variant IN ('visasuperpremiumdebit','visapremiumdebit','mcsuperpremiumdebit','visacorporatedebit','mcpremiumdebit','visacommercialpremiumdebit','mccommercialdebit','mccorporatedebit','visacommercialdebit','visacommercialsuperpremiumdebit') THEN 'VC/MC Premium Debit'
            WHEN payment_method_variant IN ('discover','jcbcredit','diners') THEN 'VC/MC Int.Credit'
            WHEN payment_method_variant IN ('vpay','electron','jcbdebit','visadankort') THEN 'VC/MC Int.Debit'
            ELSE 'Other'
        END AS "Card Type",
        SUM(ABS(TTV)) as TTV,
        SUM(ABS(MSF)) as MSF,
        SUM(ABS(ACQUIRER_FEE)) as COA,
        SUM(GP) as GP
    FROM dia_db.public.rba_model_data
    WHERE {where_clause}
    GROUP BY 1, 2
    ORDER BY 1, 2
    '''
    df = pd.read_sql(query, conn)
    return df

data = get_metric_data(bu_filter, acquirer_filter, month_filter)

# Ensure all relevant columns are numeric and fill NaN with 0 before calculations
for col in ['TTV', 'MSF', 'COA', 'GP']:
    if col in data.columns:
        data[col] = pd.to_numeric(data[col], errors='coerce').fillna(0)

# Calculate grand totals for percent columns
total_ttv = data['TTV'].sum()
total_msf = data['MSF'].sum() / 1.1
total_coa = data['COA'].sum() / 1.1
total_gp = data['GP'].sum() / 1.1

# Calculate ex-GST values and percentages
data['MSF ex gst'] = data['MSF'] / 1.1
data['COA ex gst'] = data['COA'] / 1.1
data['GP ex gst'] = data['GP'] / 1.1

data['% of TTV'] = data['TTV'] / total_ttv * 100 if total_ttv else 0
data['% of MSF'] = data['MSF ex gst'] / total_msf * 100 if total_msf else 0
data['% of COA'] = data['COA ex gst'] / total_coa * 100 if total_coa else 0
data['% of GP'] = data['GP ex gst'] / total_gp * 100 if total_gp else 0

# Format columns for display
for col in ['TTV', 'MSF ex gst', 'COA ex gst', 'GP ex gst']:
    data[col] = data[col].apply(lambda x: f"{int(round(x)):,}" if x else "0")
for col in ['% of TTV', '% of MSF', '% of COA', '% of GP']:
    data[col] = data[col].apply(lambda x: f"{x:.2f}%" if x else "0.00%")

# Select and reorder columns for display
display_columns = [
    'Business Unit',
    'Card Type',
    'TTV', '% of TTV',
    'MSF ex gst', '% of MSF',
    'COA ex gst', '% of COA',
    'GP ex gst', '% of GP'
]

# Create total row
total_row = pd.DataFrame([{
    'Business Unit': 'Total',
    'Card Type': '',
    'TTV': f"{int(round(total_ttv)):,}",
    '% of TTV': "100.00%",
    'MSF ex gst': f"{int(round(total_msf)):,}",
    '% of MSF': "100.00%",
    'COA ex gst': f"{int(round(total_coa)):,}",
    '% of COA': "100.00%",
    'GP ex gst': f"{int(round(total_gp)):,}",
    '% of GP': "100.00%"
}])

# Only group by Business Unit, not Card Type
gb = GridOptionsBuilder.from_dataframe(data[display_columns])
gb.configure_column('Business Unit', rowGroup=True, hide=True)
# Card Type is a regular column, not grouped
for col in ['TTV', '% of TTV', 'MSF ex gst', '% of MSF', 'COA ex gst', '% of COA', 'GP ex gst', '% of GP']:
    gb.configure_column(col)
gb.configure_grid_options(groupDefaultExpanded=0)
grid_options = gb.build()

# Display the grid
AgGrid(data[display_columns], gridOptions=grid_options, enable_enterprise_modules=True,
       update_mode='NO_UPDATE', use_container_width=True)

# Display total row separately
st.dataframe(total_row[display_columns], use_container_width=True) 