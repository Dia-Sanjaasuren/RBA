import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from config import SNOWFLAKE_CONFIG

@st.cache_resource
def init_snowflake_connection():
    try:
        conn = snowflake.connector.connect(
            **SNOWFLAKE_CONFIG,
            client_session_keep_alive=True
        )
        return conn
    except Exception as e:
        st.error(f"Snowflake connection failed: {str(e)}")
        return None

@st.cache_data
def get_filter_options():
    conn = init_snowflake_connection()
    df = pd.read_sql("SELECT DISTINCT SOURCE, DISPLAY_NAME, ACQUIRER, TRADING_MONTH FROM dia_db.public.rba_model_data", conn)
    business_units = sorted(df['SOURCE'].dropna().unique().tolist())
    acquirers = sorted(df['ACQUIRER'].dropna().unique().tolist())
    months = sorted(df['TRADING_MONTH'].dropna().unique().tolist())
    return business_units, acquirers, months

def get_previous_month():
    today = datetime.today()
    first = today.replace(day=1)
    prev_month = first - timedelta(days=1)
    return prev_month.strftime('%Y-%m')

def get_selected_or_all(selected, all_values):
    return all_values if "All" in selected or not selected else selected

def get_tree_metric_data(bu_list, acquirer_list, month_list):
    conn = init_snowflake_connection()
    where = []
    if bu_list and len(bu_list) < len(business_units):
        where.append(f"SOURCE IN ({','.join([repr(x) for x in bu_list])})")
    if acquirer_list and len(acquirer_list) < len(acquirers):
        where.append(f"ACQUIRER IN ({','.join([repr(x) for x in acquirer_list])})")
    if month_list and len(month_list) < len(months_desc):
        where.append(f"TRADING_MONTH IN ({','.join([repr(x) for x in month_list])})")
    where_clause = " AND ".join(where) if where else "1=1"

    query = f"""
    SELECT 
        CASE 
            WHEN SOURCE = 'Swiftpos_Reseller' THEN 'SwiftPOS Reseller'
            WHEN SOURCE = 'OolioPay' THEN 'Oolio Pay'
            WHEN SOURCE = 'IdealPOS_Reseller' THEN 'IdealPOS Reseller'
            WHEN SOURCE IN ('Oolio', 'OolioPaymentPlatform') THEN 'Oolio Platform'
            ELSE SOURCE
        END AS "Business Unit",
        DISPLAY_NAME AS DISPLAY_NAME,
        CASE 
            WHEN payment_method_variant IN ('amex','amex_applepay','amex_googlepay') THEN 'AMEX'
            WHEN payment_method_variant IN ('eftpos_australia','eftpos_australia_chq','eftpos_australia_sav') THEN 'EFTPOS'
            WHEN payment_method_variant IN ('visa','mcstandardcredit','mccredit','visastandardcredit','mc') THEN 'Domestic Credit'
            WHEN payment_method_variant IN ('visaprepaidanonymous','visastandarddebit','mcprepaidanonymous','mcdebit','maestro','mcstandarddebit') THEN 'Domestic Debit'
            WHEN payment_method_variant IN ('visapremiumcredit','mcpremiumcredit','visacorporatecredit','visacommercialpremiumcredit','mc_applepay','visacommercialsuperpremiumcredit','visa_applepay','mccorporatecredit','visa_googlepay','mccommercialcredit','mcfleetcredit','visabusiness','mcpurchasingcredit','visapurchasingcredit','mc_googlepay','visasuperpremiumcredit','visacommercialcredit','visafleetcredit','mcsuperpremiumcredit') THEN 'Premium Credit'
            WHEN payment_method_variant IN ('visasuperpremiumdebit','visapremiumdebit','mcsuperpremiumdebit','visacorporatedebit','mcpremiumdebit','visacommercialpremiumdebit','mccommercialdebit','mccorporatedebit','visacommercialdebit','visacommercialsuperpremiumdebit') THEN 'Premium Debit'
            WHEN payment_method_variant IN ('discover','jcbcredit','diners') THEN 'Int.Credit'
            WHEN payment_method_variant IN ('vpay','electron','jcbdebit','visadankort') THEN 'Int.Debit'
            ELSE 'Other'
        END AS "Card Type",
        ACQUIRER,
        SUM(ABS(TTV)) AS TTV,
        SUM(ABS(MSF)) AS MSF,
        SUM(ABS(ACQUIRER_FEE)) AS COA,
        SUM(ABS(GP)) AS GP
    FROM dia_db.public.rba_model_data
    WHERE {where_clause}
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2, 3
    """
    return pd.read_sql(query, conn)

# UI Setup
st.subheader("Business Unit Summary with Details")

# Load filters
business_units, acquirers, months = get_filter_options()
months_desc = sorted(months, reverse=True)

# Sidebar filters
default_month = get_previous_month()
col1, col2, col3 = st.columns(3)
with col1:
    selected_bu = st.multiselect("Business Unit", ["All"] + business_units, default=["All"])
with col2:
    selected_acquirer = st.multiselect("Acquirer", ["All"] + acquirers, default=["All"])
with col3:
    selected_months = st.multiselect("Month", ["All"] + months_desc, default=[default_month])

# Process filters
bu_filter = get_selected_or_all(selected_bu, business_units)
acquirer_filter = get_selected_or_all(selected_acquirer, acquirers)
month_filter = get_selected_or_all(selected_months, months_desc)

# Get data
df_raw = get_tree_metric_data(bu_filter, acquirer_filter, month_filter)

# Format tree structure
df_raw['MSF ex GST'] = df_raw['MSF'] / 1.1
df_raw['COA ex GST'] = df_raw['COA'] / 1.1
df_raw['GP ex GST'] = df_raw['GP'] / 1.1

df_raw['TTV'] = df_raw['TTV'].fillna(0).round(0).astype(int)
df_raw['MSF ex GST'] = df_raw['MSF ex GST'].fillna(0).round(0).astype(int)
df_raw['COA ex GST'] = df_raw['COA ex GST'].fillna(0).round(0).astype(int)
df_raw['GP ex GST'] = df_raw['GP ex GST'].fillna(0).round(0).astype(int)

# --- Wpay card type percentages (from TTV/assumptions) ---
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
wpay_card_type_order = [x[0] for x in wpay_card_types]

# Specify the desired card type order
card_type_order = wpay_card_type_order + ["Other"]
# Set any card types not in the list to 'Other'
df_raw['Card Type'] = df_raw['Card Type'].apply(lambda x: x if x in wpay_card_type_order else 'Other')
df_raw['Card Type'] = pd.Categorical(df_raw['Card Type'], categories=card_type_order, ordered=True)
df_raw = df_raw.sort_values(['Business Unit', 'DISPLAY_NAME', 'Card Type'])

# --- Wpay logic for card type splits ---
def apply_wpay_split(df, value_col):
    # For each BU + DISPLAY_NAME, if acquirer is Wpay, split total by wpay_card_types
    wpay_rows = []
    # Get all Wpay rows
    wpay_data = df[df['ACQUIRER'].str.contains('wpay', case=False, na=False)]
    
    # Group by Business Unit and DISPLAY_NAME
    for (bu, disp), group in wpay_data.groupby(['Business Unit', 'DISPLAY_NAME']):
        # Get the total value for this group
        total = group[value_col].sum()
        if total > 0:  # Only process if there's actual data
            # Create a row for each card type with its percentage
            for card_type, pct in wpay_card_types:
                wpay_rows.append({
                    'Business Unit': bu,
                    'DISPLAY_NAME': disp,
                    'Card Type': card_type,
                    value_col: int(round(total * pct)),
                    'ACQUIRER': 'wpay'
                })
    return pd.DataFrame(wpay_rows)

# Process Wpay data
# First, get the original Wpay data
wpay_data = df_raw[df_raw['ACQUIRER'].str.contains('wpay', case=False, na=False)].copy()

# Remove Wpay rows from df_raw
df_raw = df_raw[~df_raw['ACQUIRER'].str.contains('wpay', case=False, na=False)]

# Process each metric for Wpay
for value_col in ['TTV', 'MSF', 'COA', 'GP']:
    # Get Wpay split data
    wpay_split = apply_wpay_split(wpay_data, value_col)
    if not wpay_split.empty:
        # Append split rows
        df_raw = pd.concat([df_raw, wpay_split], ignore_index=True)

# Re-apply card type order and sort
for col in ['TTV', 'MSF', 'COA', 'GP']:
    df_raw[col] = df_raw[col].fillna(0).round(0).astype(int)

# Ensure proper ordering
# Drop ACQUIRER before grouping so Wpay and Adyen are summed together
if 'ACQUIRER' in df_raw.columns:
    df_raw = df_raw.drop(columns=['ACQUIRER'])

group_cols = ['Business Unit', 'DISPLAY_NAME', 'Card Type']
metric_cols = ['TTV', 'MSF', 'COA', 'GP', 'MSF ex GST', 'COA ex GST', 'GP ex GST']
agg_dict = {col: 'sum' for col in metric_cols if col in df_raw.columns}
df_raw = df_raw.groupby(group_cols, as_index=False).agg(agg_dict)

# Set Card Type as categorical and sort
df_raw['Card Type'] = pd.Categorical(df_raw['Card Type'], categories=card_type_order, ordered=True)
df_raw = df_raw.sort_values(['Business Unit', 'DISPLAY_NAME', 'Card Type'])

# Calculate grand totals for each metric
metric_cols = ['TTV', 'MSF', 'COA', 'GP']
grand_totals = {col: df_raw[col].sum() for col in metric_cols}

# Add % of total columns to df_raw
for col in metric_cols:
    pct_col = f'% of {col}'
    df_raw[pct_col] = df_raw[col] / grand_totals[col] * 100 if grand_totals[col] else 0
    df_raw[pct_col] = df_raw[pct_col].map('{:.2f}%'.format)

# Set the columns and headers as specified
cols = [
    'Business Unit', 'DISPLAY_NAME', 'Card Type',
    'TTV', '% of TTV',
    'MSF ex GST', '% of MSF',
    'COA ex GST', '% of COA',
    'GP ex GST', '% of GP'
]
cols = [col for col in cols if col in df_raw.columns]

gb = GridOptionsBuilder.from_dataframe(df_raw[cols])
gb.configure_column('Business Unit', rowGroup=True, hide=True, headerName='Business Unit')
gb.configure_column('DISPLAY_NAME', rowGroup=True, hide=True, headerName='Display Name')
gb.configure_column('Card Type', headerName='Card Type')
gb.configure_column('TTV', headerName='TTV', aggFunc='sum', valueFormatter="x == null ? '' : x.toLocaleString()")
gb.configure_column('% of TTV', headerName='% of TTV', aggFunc='last', valueFormatter="x == null ? '' : (Number(x.replace('%','')).toFixed(2) + '%')")
gb.configure_column('MSF ex GST', headerName='MSF ex gst', aggFunc='sum', valueFormatter="x == null ? '' : x.toLocaleString()")
gb.configure_column('% of MSF', headerName='% of MSF', aggFunc='last', valueFormatter="x == null ? '' : (Number(x.replace('%','')).toFixed(2) + '%')")
gb.configure_column('COA ex GST', headerName='COA ex gst', aggFunc='sum', valueFormatter="x == null ? '' : x.toLocaleString()")
gb.configure_column('% of COA', headerName='% of COA', aggFunc='last', valueFormatter="x == null ? '' : (Number(x.replace('%','')).toFixed(2) + '%')")
gb.configure_column('GP ex GST', headerName='GP ex gst', aggFunc='sum', valueFormatter="x == null ? '' : x.toLocaleString()")
gb.configure_column('% of GP', headerName='% of GP', aggFunc='last', valueFormatter="x == null ? '' : (Number(x.replace('%','')).toFixed(2) + '%')")
gb.configure_grid_options(groupDisplayName='Business Unit', groupDefaultExpanded=0)
grid_options = gb.build()

AgGrid(df_raw[cols], gridOptions=grid_options, enable_enterprise_modules=True,
       update_mode=GridUpdateMode.NO_UPDATE, use_container_width=True)
