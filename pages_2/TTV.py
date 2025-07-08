import os
# Disable OCSP checks to avoid certificate validation errors
os.environ['SF_OCSP_RESPONSE_CACHE_DIR'] = ''
os.environ['SF_OCSP_RESPONSE_CACHE_SERVER_ENABLED'] = 'false'
os.environ['SF_OCSP_ACTIVATE_ENDPOINT'] = 'false'

import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from config import SNOWFLAKE_CONFIG

# Page title
st.subheader("TTV Summary by Card Type")

# Make Streamlit main content area and AgGrid table full width (match TTV.py)
# (Remove or comment out this block if present)
# st.markdown("""
#     <style>
#     .main .block-container {
#         max-width: 100vw !important;
#         padding-left: 2vw;
#         padding-right: 2vw;
#     }
#     .ag-theme-material .ag-root, .ag-theme-material .ag-center-cols-clipper {
#         min-width: 2200px !important;
#         width: 100vw !important;
#         max-width: 100vw !important;
#     }
#     </style>
# """, unsafe_allow_html=True)

# Define payment method order for all tables
payment_method_order = [
    'AMEX',
    'EFTPOS',
    'Dom.DR',
    'Dom.CR',
    'Prem.DR',
    'Prem.CR',
    'Int.DR',
    'Int.CR'
]

# Define Wpay card type percentages
wpay_card_types = [
    ("AMEX", 0.0140),
    ("EFTPOS", 0.3208),
    ("Dom.DR", 0.2500),
    ("Dom.CR", 0.1850),
    ("Prem.DR", 0.0952),
    ("Prem.CR", 0.1000),
    ("Int.DR", 0.0050),
    ("Int.CR", 0.0300)
]

def format_currency(value):
    if pd.isna(value) or value == 0:
        return "$0"
    return f"${value:,.0f}"

def get_previous_month():
    today = datetime.today()
    first = today.replace(day=1)
    prev_month = first - timedelta(days=1)
    return prev_month.strftime('%Y-%m')

@st.cache_resource
def init_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

@st.cache_data
def get_filter_options():
    conn = init_snowflake_connection()
    df = pd.read_sql("SELECT DISTINCT SOURCE, ACQUIRER, TRADING_MONTH FROM dia_db.public.rba_model_data", conn)
    business_units = sorted(df['SOURCE'].dropna().unique().tolist())
    acquirers = sorted(df['ACQUIRER'].dropna().unique().tolist())
    months = sorted(df['TRADING_MONTH'].dropna().unique().tolist())
    return business_units, acquirers, months

acquirer_display_map = {
    'adyen_managed': 'Adyen Managed',
    'adyen_balance': 'Adyen Balance',
    'wpay': 'Wpay'
}
acquirer_display_reverse = {v: k for k, v in acquirer_display_map.items()}

business_units, acquirers, months = get_filter_options()
default_month = get_previous_month()
months_desc = sorted(months, reverse=True)
all_bu = ["All"] + business_units
all_acquirer = ["All"] + [acquirer_display_map.get(a, a) for a in acquirers]
all_months = ["All"] + months_desc

# Updated filter layout to match MSF/COA (larger, default columns)
col1, col2, col3 = st.columns(3)
with col1:
    selected_bu = st.multiselect("Business Unit", all_bu, default=["All"])
with col2:
    selected_acquirer = st.multiselect("Acquirer", all_acquirer, default=["All"])
with col3:
    selected_months = st.multiselect("Trading Month", all_months, default=[default_month])

selected_acquirer_internal = [acquirer_display_reverse.get(a, a) for a in selected_acquirer]

def get_selected_or_all(selected, all_values):
    if "All" in selected or not selected:
        return all_values
    return selected

bu_filter = get_selected_or_all(selected_bu, business_units)
acquirer_filter = get_selected_or_all(selected_acquirer_internal, acquirers)
month_filter = get_selected_or_all(selected_months, months_desc)

def get_metric_data(bu_list, acquirer_list, month_list):
    conn = init_snowflake_connection()
    where_clauses = []
    if bu_list and len(bu_list) < len(business_units):
        bu_str = ", ".join([f"'{b}'" for b in bu_list])
        where_clauses.append(f"SOURCE IN ({bu_str})")
    if acquirer_list and len(acquirer_list) < len(acquirers):
        acq_str = ", ".join([f"'{a}'" for a in acquirer_list])
        where_clauses.append(f"LOWER(ACQUIRER) IN ({acq_str})")
    if month_list and len(month_list) < len(months_desc):
        month_str = ", ".join([f"'{m}'" for m in month_list])
        where_clauses.append(f"TRADING_MONTH IN ({month_str})")
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"
    query = f"""
    WITH base_data AS (
        SELECT 
            CASE 
                WHEN SOURCE = 'Swiftpos_Reseller' THEN 'SwiftPOS Res'
                WHEN SOURCE = 'OolioPay' THEN 'Oolio Pay'
                WHEN SOURCE = 'IdealPOS_Reseller' THEN 'IdealPOS Res'
                WHEN SOURCE IN ('Oolio', 'OolioPaymentPlatform') THEN 'Oolio Platform'
                WHEN SOURCE = 'Deliverit' THEN 'Deliverit'
                WHEN SOURCE = 'DeliverIT MoR' THEN 'DeliverIT MoR'
                ELSE SOURCE
            END AS "Business Unit",
            CASE 
                WHEN payment_method_variant IN ('amex','amex_applepay','amex_googlepay') THEN 'AMEX'
                WHEN payment_method_variant IN ('eftpos_australia','eftpos_australia_chq','eftpos_australia_sav') THEN 'EFTPOS'
                WHEN payment_method_variant IN ('visa','mcstandardcredit','mccredit','visastandardcredit','mc') THEN 'Dom.CR'
                WHEN payment_method_variant IN ('visaprepaidanonymous','visastandarddebit','mcprepaidanonymous','mcdebit','maestro','mcstandarddebit') THEN 'Dom.DR'
                WHEN payment_method_variant IN ('visapremiumcredit','mcpremiumcredit','visacorporatecredit','visacommercialpremiumcredit','mc_applepay','visacommercialsuperpremiumcredit','visa_applepay','mccorporatecredit','visa_googlepay','mccommercialcredit','mcfleetcredit','visabusiness','mcpurchasingcredit','visapurchasingcredit','mc_googlepay','visasuperpremiumcredit','visacommercialcredit','visafleetcredit','mcsuperpremiumcredit') THEN 'Prem.CR'
                WHEN payment_method_variant IN ('visasuperpremiumdebit','visapremiumdebit','mcsuperpremiumdebit','visacorporatedebit','mcpremiumdebit','visacommercialpremiumdebit','mccommercialdebit','mccorporatedebit','visacommercialdebit','visacommercialsuperpremiumdebit') THEN 'Prem.DR'
                WHEN payment_method_variant IN ('discover','jcbcredit','diners') THEN 'Int.CR'
                WHEN payment_method_variant IN ('vpay','electron','jcbdebit','visadankort') THEN 'Int.DR'
                ELSE 'Other'
            END AS "Card Type",
            PAYMENT_METHOD,
            payment_method_variant,
            LOWER(ACQUIRER) as ACQUIRER,
            SUM(ABS(TTV)) as VALUE
        FROM dia_db.public.rba_model_data
        WHERE {where_clause}
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT 
        "Business Unit",
        "Card Type",
        PAYMENT_METHOD,
        payment_method_variant,
        ACQUIRER,
        SUM(VALUE) as VALUE
    FROM base_data
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY "Business Unit", "Card Type"
    """
    df = pd.read_sql(query, conn)
    return df

data = get_metric_data(bu_filter, acquirer_filter, month_filter)

# --- SUMMARY METRICS ---
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Business Units", len(data['Business Unit'].unique()))
with col2:
    st.metric("Total Payment Methods", len(payment_method_order))
with col3:
    st.metric("Total TTV", format_currency(data['VALUE'].sum()))

# Build a combined table for all acquirers, with Wpay split: AMEX/EFTPOS direct, rest split among 6 types
rows = []
grand_total = 0
other_card_types = ['Dom.DR', 'Dom.CR', 'Prem.DR', 'Prem.CR', 'Int.DR', 'Int.CR']
other_weights = [25, 18.5, 10, 9.52, 3, 0.5]
sum_weights = sum(other_weights)
for bu in data['Business Unit'].unique():
    bu_data = data[data['Business Unit'] == bu]
    row = {'Business Unit': bu}
    total = 0
    card_type_values = {}
    # --- Wpay split logic ---
    wpay_data = bu_data[bu_data['ACQUIRER'].str.contains('wpay', case=False)]
    wpay_total = wpay_data['VALUE'].sum()
    wpay_amex = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['VALUE'].sum()
    wpay_eftpos = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['VALUE'].sum()
    wpay_rest = wpay_total - wpay_amex - wpay_eftpos
    # Normalized weights for the 6 card types
    other_card_types = ['Dom.DR', 'Dom.CR', 'Prem.CR', 'Prem.DR', 'Int.CR', 'Int.DR']
    other_weights = [25, 18.5, 10, 9.52, 3, 0.5]
    sum_weights = sum(other_weights)
    wpay_split = {ct: 0 for ct in payment_method_order}
    wpay_split['AMEX'] = wpay_amex
    wpay_split['EFTPOS'] = wpay_eftpos
    for ct, w in zip(other_card_types, other_weights):
        wpay_split[ct] = wpay_rest * (w / sum_weights) if wpay_rest > 0 else 0
    # --- Sum all acquirers ---
    for card_type in payment_method_order:
        adyen_managed = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['VALUE'].sum()
        adyen_balance = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['VALUE'].sum()
        wpay_value = wpay_split[card_type]
        value = adyen_managed + adyen_balance + wpay_value
        card_type_values[card_type] = value
        total += value
    row.update(card_type_values)
    row['Total'] = total
    grand_total += total
    rows.append(row)

# Add Total row
total_row = {'Business Unit': 'Total'}
total_sum = 0
for card_type in payment_method_order:
    value = sum(row[card_type] for row in rows)
    total_row[card_type] = value
    total_sum += value
total_row['Total'] = total_sum
rows.append(total_row)

# Create DataFrame with both values and percentages
final_rows = []
for row in rows[:-1]:
    dollar_row = {'Business Unit': row['Business Unit']}
    for card_type in payment_method_order:
        dollar_row[card_type] = format_currency(row[card_type])
    dollar_row['Total'] = format_currency(row['Total'])
    dollar_row['% Of Total'] = f"{(row['Total'] / grand_total * 100):.2f}%"
    final_rows.append(dollar_row)
    pct_row = {'Business Unit': '% of TTV'}
    for card_type in payment_method_order:
        pct = (row[card_type] / row['Total'] * 100) if row['Total'] > 0 else 0
        pct_row[card_type] = f"{pct:.2f}%"
    pct_row['Total'] = "100.00%"
    pct_row['% Of Total'] = ""
    final_rows.append(pct_row)

total_dollar_row = {'Business Unit': 'Total'}
for card_type in payment_method_order:
    total_dollar_row[card_type] = format_currency(total_row[card_type])
total_dollar_row['Total'] = format_currency(total_sum)
total_dollar_row['% Of Total'] = "100.00%"
final_rows.append(total_dollar_row)
total_pct_row = {'Business Unit': '% of Total'}
for card_type in payment_method_order:
    pct = (total_row[card_type] / total_sum * 100) if total_sum > 0 else 0
    total_pct_row[card_type] = f"{pct:.2f}%"
total_pct_row['Total'] = "100.00%"
total_pct_row['% Of Total'] = ""
final_rows.append(total_pct_row)
final_df = pd.DataFrame(final_rows)

# Configure grid options (match TTV.py)
gb = GridOptionsBuilder.from_dataframe(final_df)
gb.configure_default_column(
    resizable=True,
    sorteable=False,
    filterable=False,
    groupable=False
)
# Remove fixed widths for columns to allow auto-sizing
gb.configure_column("Business Unit", pinned="left")
# No width set for payment columns, let AgGrid auto-size
# for col in payment_method_order:
#     gb.configure_column(col, width=120)
# gb.configure_column("Total", width=120)
# gb.configure_column("% Of Total", width=100)

# Custom cell style for font size and padding (match TTV.py)
gb.configure_grid_options(
    suppressRowTransform=True,
    domLayout='normal',  # Use normal for scrollable table
)
grid_options = gb.build()

# Add custom CSS for larger filter widgets and table font
st.markdown("""
    <style>
    .ag-theme-material .ag-cell {
        padding: 2px 4px !important;
        font-size: 10px !important;
    }
    .ag-theme-material .ag-header-cell {
        padding: 2px 4px !important;
        font-size: 10px !important;
    }
    .stMultiSelect, .stSelectbox, .stTextInput, .stDateInput, .stNumberInput {
        font-size: 15px !important;
        min-width: 160px !important;
        max-width: 320px !important;
    }
    @media (max-width: 700px) {
        .ag-theme-material .ag-cell, .ag-theme-material .ag-header-cell {
            font-size: 9px !important;
            padding: 1px 2px !important;
        }
        .stMultiSelect, .stSelectbox, .stTextInput, .stDateInput, .stNumberInput {
            font-size: 12px !important;
            min-width: 100px !important;
            max-width: 160px !important;
        }
    }
    </style>
""", unsafe_allow_html=True)

# Display the table using AgGrid (compact, content-width only)
AgGrid(
    final_df,
    gridOptions=grid_options,
    allow_unsafe_jscode=True,
    theme="material",
    height=500,  # Set a fixed height for scroll
    # fit_columns_on_grid_load=True,  # as needed
    # use_container_width=True,       # as needed
)

# Add small italic note below the table and above the summary
st.markdown('<div style="text-align:right; font-size:12px;"><i>Note: Wpay card type distributions are based on market assumptions, except for AMEX and EFTPOS, which use actual data.</i></div>', unsafe_allow_html=True)