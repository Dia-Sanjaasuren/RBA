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
show_bips = st.toggle("Show in Bips", value=False, help="Toggle between dollar values and basis points (Bips = MSF/TTV*10000)")

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
            SUM(MSF) as MSF_VALUE,
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

# Process the data
rows = []
grand_total = 0
grand_total_ttv = 0
other_card_types = ['Dom.DR', 'Dom.CR', 'Prem.DR', 'Prem.CR', 'Int.DR', 'Int.CR']
other_weights = [25, 18.5, 10, 9.52, 3, 0.5]
sum_weights = sum(other_weights)
for bu in data['Business Unit'].unique():
    bu_data = data[data['Business Unit'] == bu]
    row = {'Business Unit': bu}
    total_msf = 0
    total_ttv = 0
    total_surcharge = 0
    card_type_values = {}
    # --- Wpay split logic ---
    wpay_data = bu_data[bu_data['ACQUIRER'].str.contains('wpay', case=False)]
    wpay_total = wpay_data['MSF_VALUE'].sum()
    wpay_total_ttv = wpay_data['TTV_VALUE'].sum()
    wpay_total_surcharge = wpay_data['SURCHARGE_VALUE'].sum()
    wpay_amex = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['MSF_VALUE'].sum()
    wpay_amex_ttv = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['TTV_VALUE'].sum()
    wpay_amex_surcharge = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['SURCHARGE_VALUE'].sum()
    wpay_eftpos = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['MSF_VALUE'].sum()
    wpay_eftpos_ttv = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['TTV_VALUE'].sum()
    wpay_eftpos_surcharge = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['SURCHARGE_VALUE'].sum()
    wpay_rest = wpay_total - wpay_amex - wpay_eftpos
    wpay_rest_ttv = wpay_total_ttv - wpay_amex_ttv - wpay_eftpos_ttv
    wpay_rest_surcharge = wpay_total_surcharge - wpay_amex_surcharge - wpay_eftpos_surcharge
    wpay_split = {ct: 0 for ct in payment_method_order}
    wpay_split_ttv = {ct: 0 for ct in payment_method_order}
    wpay_split_surcharge = {ct: 0 for ct in payment_method_order}
    wpay_split['AMEX'] = wpay_amex
    wpay_split['EFTPOS'] = wpay_eftpos
    wpay_split_ttv['AMEX'] = wpay_amex_ttv
    wpay_split_ttv['EFTPOS'] = wpay_eftpos_ttv
    wpay_split_surcharge['AMEX'] = wpay_amex_surcharge
    wpay_split_surcharge['EFTPOS'] = wpay_eftpos_surcharge
    for ct, w in zip(other_card_types, other_weights):
        wpay_split[ct] = wpay_rest * (w / sum_weights) if wpay_rest > 0 else 0
        wpay_split_ttv[ct] = wpay_rest_ttv * (w / sum_weights) if wpay_rest_ttv > 0 else 0
        wpay_split_surcharge[ct] = wpay_rest_surcharge * (w / sum_weights) if wpay_rest_surcharge > 0 else 0
    # --- Sum all acquirers ---
    for card_type in payment_method_order:
        adyen_managed = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['MSF_VALUE'].sum()
        adyen_managed_ttv = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['TTV_VALUE'].sum()
        adyen_managed_surcharge = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['SURCHARGE_VALUE'].sum()
        adyen_balance = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['MSF_VALUE'].sum()
        adyen_balance_ttv = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['TTV_VALUE'].sum()
        adyen_balance_surcharge = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['SURCHARGE_VALUE'].sum()
        wpay_value = wpay_split[card_type]
        wpay_value_ttv = wpay_split_ttv[card_type]
        wpay_value_surcharge = wpay_split_surcharge[card_type]
        msf_value = (adyen_managed + adyen_balance + wpay_value) / 1.1
        ttv_value = adyen_managed_ttv + adyen_balance_ttv + wpay_value_ttv
        surcharge_value = adyen_managed_surcharge + adyen_balance_surcharge + wpay_value_surcharge
        denominator = ttv_value - surcharge_value
        if show_bips and denominator > 0:
            value = (msf_value / denominator) * 10000
        else:
            value = msf_value
        card_type_values[card_type] = value
        total_msf += msf_value
        total_ttv += ttv_value
        total_surcharge += surcharge_value
    if show_bips and (total_ttv - total_surcharge) > 0:
        row['Total'] = (total_msf / (total_ttv - total_surcharge)) * 10000
    else:
        row['Total'] = sum(card_type_values.values())
    row.update(card_type_values)
    grand_total += row['Total']
    grand_total_ttv += total_ttv
    rows.append(row)

total_row = {'Business Unit': 'Total'}
total_sum = 0
for card_type in payment_method_order:
    if acquirer_filter == ["Wpay"]:
        pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
        msf = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['MSF_VALUE'].sum() * pct
        ttv = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['TTV_VALUE'].sum() * pct
        surcharge = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['SURCHARGE_VALUE'].sum() * pct
    elif acquirer_filter == ["Adyen Managed"]:
        msf = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['MSF_VALUE'].sum()
        ttv = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['TTV_VALUE'].sum()
        surcharge = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['SURCHARGE_VALUE'].sum()
    elif acquirer_filter == ["Adyen Balance"]:
        msf = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['MSF_VALUE'].sum()
        ttv = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['TTV_VALUE'].sum()
        surcharge = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['SURCHARGE_VALUE'].sum()
    else:
        adyen_managed_msf = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['MSF_VALUE'].sum()
        adyen_managed_ttv = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['TTV_VALUE'].sum()
        adyen_managed_surcharge = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['SURCHARGE_VALUE'].sum()
        adyen_balance_msf = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['MSF_VALUE'].sum()
        adyen_balance_ttv = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['TTV_VALUE'].sum()
        adyen_balance_surcharge = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['SURCHARGE_VALUE'].sum()
        wpay_total_msf = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['MSF_VALUE'].sum()
        wpay_total_ttv = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['TTV_VALUE'].sum()
        wpay_total_surcharge = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['SURCHARGE_VALUE'].sum()
        wpay_pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
        wpay_msf = wpay_total_msf * wpay_pct
        wpay_ttv = wpay_total_ttv * wpay_pct
        wpay_surcharge = wpay_total_surcharge * wpay_pct
        msf = adyen_managed_msf + adyen_balance_msf + wpay_msf
        ttv = adyen_managed_ttv + adyen_balance_ttv + wpay_ttv
        surcharge = adyen_managed_surcharge + adyen_balance_surcharge + wpay_surcharge
    denominator = ttv - surcharge
    msf_ex_gst = msf / 1.1
    if show_bips and denominator > 0:
        value = (msf_ex_gst / denominator) * 10000
    else:
        value = msf_ex_gst
    total_row[card_type] = value
    total_sum += value
total_row['Total'] = total_sum
rows.append(total_row)

# --- Calculate all percent rows and % Of Total column from dollar values (before any BIPS logic) ---
# Prepare a list of percent rows and % Of Total values for each business unit, based on dollar values
percent_rows = []
percent_of_total_col = {}
total_msf_sum = sum(row['Total'] for row in rows[:-1])
for row in rows[:-1]:
    pct_row = {'Business Unit': '% of MSF'}
    for card_type in payment_method_order:
        pct = (row[card_type] / row['Total'] * 100) if row['Total'] > 0 else 0
        pct_row[card_type] = f"{pct:.2f}%"
    pct_row['Total'] = "100.00%"
    percent_rows.append(pct_row)
    percent_of_total_col[row['Business Unit']] = f"{(row['Total'] / total_msf_sum * 100):.2f}%" if total_msf_sum > 0 else "0.00%"

# --- Now build the final_rows for display ---
final_rows = []
for i, row in enumerate(rows[:-1]):
    # Value row (dollar or BIPS)
    dollar_row = {'Business Unit': row['Business Unit']}
    for card_type in payment_method_order:
        if show_bips:
            dollar_row[card_type] = f"{row[card_type]:.2f}"
        else:
            dollar_row[card_type] = format_currency(row[card_type])
    if show_bips:
        dollar_row['Total'] = f"{row['Total']:.2f}"
    else:
        dollar_row['Total'] = format_currency(row['Total'])
    # Always use percent_of_total_col calculated from dollar values
    dollar_row['% Of Total'] = percent_of_total_col[row['Business Unit']]
    final_rows.append(dollar_row)
    # Percent row (always from dollar values)
    final_rows.append(percent_rows[i])

total_dollar_row = {'Business Unit': 'Total'}
for card_type in payment_method_order:
    if show_bips:
        total_dollar_row[card_type] = f"{total_row[card_type]:.2f}"
    else:
        total_dollar_row[card_type] = format_currency(total_row[card_type])
if show_bips:
    total_dollar_row['Total'] = f"{total_sum:.2f}"
else:
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

# Add custom CSS for smallest font and responsive compactness
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
    @media (max-width: 700px) {
        .ag-theme-material .ag-cell, .ag-theme-material .ag-header-cell {
            font-size: 9px !important;
            padding: 1px 2px !important;
        }
    }
    </style>
""", unsafe_allow_html=True)

# Display the table using AgGrid (compact, content-width only)
AgGrid(
    final_df,
    gridOptions=gb.build(),
    theme="material",
    height=500,
    # fit_columns_on_grid_load=True,  # Removed for content-width only
    # use_container_width=True,       # Removed for content-width only
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
    if show_bips:
        st.metric("Average Bips", f"{grand_total/len(data['Business Unit'].unique()):.2f}")
    else:
        st.metric("Total MSF", format_currency(data['MSF_VALUE'].sum() / 1.1)) 