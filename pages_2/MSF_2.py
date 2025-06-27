import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from config import SNOWFLAKE_CONFIG

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
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

@st.cache_data
def get_filter_options():
    conn = init_snowflake_connection()
    df = pd.read_sql("SELECT DISTINCT SOURCE, ACQUIRER, TRADING_MONTH FROM dia_db.public.rba_model_data", conn)
    business_units = sorted(df['SOURCE'].dropna().unique().tolist())
    acquirers = sorted(df['ACQUIRER'].dropna().unique().tolist())
    months = sorted(df['TRADING_MONTH'].dropna().unique().tolist())
    return business_units, acquirers, months

# Main content
# Remove the dashboard intro/title/description at the top of the MSF page

# Page title at the very top
st.subheader("MSF Summary by Card Type")

# Get filter options
business_units, acquirers, months = get_filter_options()
default_month = get_previous_month()

# --- Combined Table Filters ---
months_desc = sorted(months, reverse=True)

# Ensure all_months is defined
all_months = ["All"] + months_desc

# Add 'All' option to each filter
all_bu = ["All"] + business_units

# Updated acquirer filter options
acquirer_options = ["Adyen Balance", "Adyen Managed", "Wpay"]
all_acquirer = ["All"] + acquirer_options

# Updated filter layout to display in a single row
col1, col2, col3 = st.columns(3)
with col1:
    selected_bu = st.multiselect("Business Unit", all_bu, default=["All"])
with col2:
    selected_acquirer = st.multiselect("Acquirer", all_acquirer, default=["All"])
with col3:
    selected_months = st.multiselect("Trading Month", all_months, default=[default_month])

def get_selected_or_all(selected, all_values):
    if "All" in selected or not selected:
        return all_values
    return selected

bu_filter = get_selected_or_all(selected_bu, business_units)

# Updated acquirer filter logic
acquirer_map = {
    "Adyen Managed": "adyen_managed",
    "Adyen Balance": "adyen_balance",
    "Wpay": "wpay"
}
selected_acquirer_internal = [acquirer_map[a] for a in selected_acquirer if a in acquirer_map]
acquirer_filter = get_selected_or_all(selected_acquirer_internal, [acquirer_map[a] for a in acquirer_options if a in acquirer_map])

month_filter = get_selected_or_all(selected_months, months_desc)

# Add Bips toggle
show_bips = st.toggle("Show in Bips", value=False, help="Toggle between dollar values and basis points (Bips = MSF/(TTV-SURCHARGE_AMOUNT)*10000)")

def get_metric_data(bu_list, acquirer_list, month_list):
    conn = init_snowflake_connection()
    where_clauses = []
    if bu_list and len(bu_list) < len(business_units):
        bu_str = ", ".join([f"'{b}'" for b in bu_list])
        where_clauses.append(f"SOURCE IN ({bu_str})")
    if acquirer_list and len(acquirer_list) < len(acquirer_options):
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
                WHEN SOURCE = 'Swiftpos_Reseller' THEN 'SwiftPOS Reseller'
                WHEN SOURCE = 'OolioPay' THEN 'Oolio Pay'
                WHEN SOURCE = 'IdealPOS_Reseller' THEN 'IdealPOS Reseller'
                WHEN SOURCE IN ('Oolio', 'OolioPaymentPlatform') THEN 'Oolio Platform'
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
            LOWER(ACQUIRER) as ACQUIRER,
            SUM(ABS(MSF)) as MSF_VALUE,
            SUM(TTV) as TTV_VALUE,
            SUM(SURCHARGE_AMOUNT) as SURCHARGE_VALUE
        FROM dia_db.public.rba_model_data
        WHERE {where_clause}
        GROUP BY 1, 2, 3, 4
    )
    SELECT 
        "Business Unit",
        "Card Type",
        PAYMENT_METHOD,
        ACQUIRER,
        SUM(MSF_VALUE) as MSF_VALUE,
        SUM(TTV_VALUE) as TTV_VALUE,
        SUM(SURCHARGE_VALUE) as SURCHARGE_VALUE
    FROM base_data
    GROUP BY 1, 2, 3, 4
    ORDER BY "Business Unit", "Card Type"
    """
    df = pd.read_sql(query, conn)
    return df

data = get_metric_data(bu_filter, acquirer_filter, month_filter)

# Table logic: same as TTV, but use MSF values
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
    wpay_total = wpay_data['MSF_VALUE'].sum() if 'MSF_VALUE' in wpay_data.columns else 0
    wpay_amex = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['MSF_VALUE'].sum() if 'PAYMENT_METHOD' in wpay_data.columns and 'MSF_VALUE' in wpay_data.columns else 0
    wpay_eftpos = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['MSF_VALUE'].sum() if 'PAYMENT_METHOD' in wpay_data.columns and 'MSF_VALUE' in wpay_data.columns else 0
    wpay_rest = wpay_total - wpay_amex - wpay_eftpos
    wpay_split = {ct: 0 for ct in payment_method_order}
    wpay_split['AMEX'] = wpay_amex
    wpay_split['EFTPOS'] = wpay_eftpos
    for ct, w in zip(other_card_types, other_weights):
        wpay_split[ct] = wpay_rest * (w / sum_weights) if wpay_rest > 0 else 0
    # --- Sum all acquirers ---
    for card_type in payment_method_order:
        adyen_managed = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['MSF_VALUE'].sum() if 'MSF_VALUE' in bu_data.columns else 0
        adyen_balance = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['MSF_VALUE'].sum() if 'MSF_VALUE' in bu_data.columns else 0
        wpay_value = wpay_split[card_type]
        msf_value = adyen_managed + adyen_balance + wpay_value
        # Get TTV and Surcharge for Bips calculation
        ttv = bu_data[(bu_data['Card Type'] == card_type)]['TTV_VALUE'].sum() if 'TTV_VALUE' in bu_data.columns else 0
        surcharge = bu_data[(bu_data['Card Type'] == card_type)]['SURCHARGE_VALUE'].sum() if 'SURCHARGE_VALUE' in bu_data.columns else 0
        if show_bips:
            denominator = ttv - surcharge
            bips = (msf_value / denominator * 10000) if denominator > 0 else 0
            card_type_values[card_type] = f"{bips:.2f}"
        else:
            card_type_values[card_type] = msf_value
        total += msf_value
    row.update(card_type_values)
    row['Total'] = total
    grand_total += total
    rows.append(row)

total_row = {'Business Unit': 'Total'}
total_sum = 0
for card_type in payment_method_order:
    if acquirer_filter == ["Wpay"]:
        pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
        value = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['MSF_VALUE'].sum() * pct
    elif acquirer_filter == ["Adyen Managed"]:
        value = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['MSF_VALUE'].sum()
    elif acquirer_filter == ["Adyen Balance"]:
        value = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['MSF_VALUE'].sum()
    else:
        adyen_managed = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['MSF_VALUE'].sum()
        adyen_balance = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['MSF_VALUE'].sum()
        wpay_total = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['MSF_VALUE'].sum()
        wpay_pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
        wpay_value = wpay_total * wpay_pct
        value = adyen_managed + adyen_balance + wpay_value
    total_row[card_type] = value
    total_sum += value
total_row['Total'] = total_sum
rows.append(total_row)

final_rows = []
for row in rows[:-1]:
    dollar_row = {'Business Unit': row['Business Unit']}
    for card_type in payment_method_order:
        if show_bips:
            dollar_row[card_type] = row[card_type] if row[card_type] == '' or row[card_type] == 0 else f"{row[card_type]}"
        else:
            dollar_row[card_type] = format_currency(row[card_type])
    dollar_row['Total'] = format_currency(row['Total']) if not show_bips else ''
    dollar_row['% Of Total'] = f"{(row['Total'] / grand_total * 100):.2f}%" if not show_bips else ''
    final_rows.append(dollar_row)
    pct_row = {'Business Unit': '% of MSF'}
    for card_type in payment_method_order:
        pct = (row[card_type] / row['Total'] * 100) if row['Total'] > 0 and not show_bips else 0
        pct_row[card_type] = f"{pct:.2f}%" if not show_bips else ''
    pct_row['Total'] = "100.00%" if not show_bips else ''
    pct_row['% Of Total'] = "" if not show_bips else ''
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

# Display the table
gb = GridOptionsBuilder.from_dataframe(final_df)
gb.configure_default_column(resizable=True, sorteable=False, filterable=False, groupable=False, width=60)
gb.configure_column("Business Unit", pinned="left", width=90)
for col in payment_method_order:
    gb.configure_column(col, width=60)
gb.configure_column("Total", width=60)
gb.configure_column("% Of Total", width=60)
AgGrid(
    final_df,
    gridOptions=gb.build(),
    theme="material",
    height=500,
    fit_columns_on_grid_load=False,
    use_container_width=True,
    custom_css={
        ".ag-header-cell-label": {"justify-content": "center", "font-size": "11px"},
        ".ag-cell": {"min-width": "60px", "font-size": "11px", "padding": "0 2px"}
    }
)

# Add small italic note below the table and above the summary
st.markdown('<div style="text-align:right; font-size:12px;"><i>Note: Wpay card type distributions are based on market assumptions, except for AMEX and EFTPOS, which use actual data.</i></div>', unsafe_allow_html=True)

# Summary at the bottom
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Business Units", len(data['Business Unit'].unique()))
with col2:
    st.metric("Total Payment Methods", len(payment_method_order))
with col3:
    st.metric("Total MSF", format_currency(data['MSF_VALUE'].sum())) 