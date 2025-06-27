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
# Remove the dashboard intro/title/description at the top of the COA page

# Page title at the very top
st.subheader("GP Summary by Card Type")

# Get filter options
business_units, acquirers, months = get_filter_options()
default_month = get_previous_month()

# --- Combined Table Filters ---
months_desc = sorted(months, reverse=True)
all_months = ["All"] + months_desc
all_bu = ["All"] + business_units

# Acquirer display mapping
acquirer_display_map = {
    'adyen_managed': 'Adyen Managed',
    'adyen_balance': 'Adyen Balance',
    'wpay': 'Wpay'
}
acquirer_display_reverse = {v: k for k, v in acquirer_display_map.items()}
all_acquirer = ["All"] + [acquirer_display_map.get(a, a) for a in acquirers]

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

# Use dynamic acquirer list for filtering
selected_acquirer_internal = [acquirer_display_reverse.get(a, a) for a in selected_acquirer]
acquirer_filter = get_selected_or_all(selected_acquirer_internal, acquirers)

month_filter = get_selected_or_all(selected_months, months_desc)

# Add Bips toggle
show_bips = st.toggle("Show in Bips", value=False, help="Toggle between dollar values and basis points (Bips = GP/TTV*10000)")

def get_metric_data(bu_list, acquirer_list, month_list):
    conn = init_snowflake_connection()
    where_clauses = []
    if bu_list and "All" not in bu_list:
        bu_str = ", ".join([f"'{b}'" for b in bu_list])
        where_clauses.append(f"SOURCE IN ({bu_str})")
    if acquirer_list and "All" not in acquirer_list:
        acq_str = ", ".join([f"'{a.lower()}'" for a in acquirer_list])
        where_clauses.append(f"LOWER(ACQUIRER) IN ({acq_str})")
    if month_list and "All" not in month_list:
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
            LOWER(ACQUIRER) as ACQUIRER,
            SUM(GP) as GP_VALUE,
            SUM(TTV) as TTV_VALUE
        FROM dia_db.public.rba_model_data
        WHERE {where_clause}
        GROUP BY 1, 2, 3, 4
    )
    SELECT 
        "Business Unit",
        "Card Type",
        PAYMENT_METHOD,
        ACQUIRER,
        SUM(GP_VALUE) as GP_VALUE,
        SUM(TTV_VALUE) as TTV_VALUE
    FROM base_data
    GROUP BY 1, 2, 3, 4
    ORDER BY "Business Unit", "Card Type"
    """
    df = pd.read_sql(query, conn)
    return df

data = get_metric_data(bu_filter, acquirer_filter, month_filter)

# --- Wpay logic: construct split rows as in model_bu_2.py ---
if not data.empty:
    wpay_data = data[data['ACQUIRER'].str.contains('wpay', case=False, na=False)].copy()
    wpay_agg_rows = []
    for bu in wpay_data['Business Unit'].unique():
        bu_wpay_data = wpay_data[wpay_data['Business Unit'] == bu]
        wpay_amex = bu_wpay_data[bu_wpay_data['PAYMENT_METHOD'] == 'AMEX'].sum(numeric_only=True)
        wpay_eftpos = bu_wpay_data[bu_wpay_data['PAYMENT_METHOD'] == 'EFTPOS'].sum(numeric_only=True)
        wpay_total = bu_wpay_data.sum(numeric_only=True)
        wpay_rest = wpay_total - wpay_amex - wpay_eftpos
        if wpay_amex['GP_VALUE'] > 0:
            wpay_agg_rows.append({'Business Unit': bu, 'Card Type': 'AMEX', **wpay_amex})
        if wpay_eftpos['GP_VALUE'] > 0:
            wpay_agg_rows.append({'Business Unit': bu, 'Card Type': 'EFTPOS', **wpay_eftpos})
        other_card_types = {'Dom.DR': 0.25, 'Dom.CR': 0.185, 'Prem.DR': 0.0952, 'Prem.CR': 0.10, 'Int.DR': 0.005, 'Int.CR': 0.03}
        total_weight = sum(other_card_types.values())
        if wpay_rest['GP_VALUE'] > 0 and total_weight > 0:
            for card, weight in other_card_types.items():
                prorated_rest = (wpay_rest * (weight / total_weight))
                wpay_agg_rows.append({'Business Unit': bu, 'Card Type': card, **prorated_rest})
    wpay_agg = pd.DataFrame(wpay_agg_rows)
    # Remove original Wpay rows and append split rows
    data = pd.concat([data[~data['ACQUIRER'].str.contains('wpay', case=False, na=False)], wpay_agg], ignore_index=True)
    # Ensure Card Type and Business Unit columns exist and have no NaN after Wpay split
    if 'Card Type' not in data.columns:
        data['Card Type'] = ''
    else:
        data['Card Type'] = data['Card Type'].fillna('')
    if 'Business Unit' not in data.columns:
        data['Business Unit'] = ''
    else:
        data['Business Unit'] = data['Business Unit'].fillna('')

# --- SUMMARY METRICS ---
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Business Units", len(data['Business Unit'].unique()))
with col2:
    st.metric("Total Payment Methods", len(payment_method_order))
with col3:
    st.metric("Total GP (ex GST)", format_currency(data['GP_VALUE'].sum()))

# Table logic: same as TTV, but use GP values
rows = []
grand_total = 0
other_card_types = ['Dom.DR', 'Dom.CR', 'Prem.DR', 'Prem.CR', 'Int.DR', 'Int.CR']
other_weights = [25, 18.5, 10, 9.52, 3, 0.5]
sum_weights = sum(other_weights)
for bu in data['Business Unit'].unique():
    bu_data = data[data['Business Unit'] == bu]
    row = {'Business Unit': bu}
    total_gp = 0
    total_ttv = 0
    card_type_values = {}
    # --- Wpay split logic ---
    wpay_data = bu_data[bu_data['ACQUIRER'].str.contains('wpay', case=False)]
    wpay_total = wpay_data['GP_VALUE'].sum()
    wpay_total_ttv = wpay_data['TTV_VALUE'].sum() if 'TTV_VALUE' in wpay_data.columns else 0
    wpay_amex = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['GP_VALUE'].sum() if 'PAYMENT_METHOD' in wpay_data.columns else 0
    wpay_amex_ttv = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['TTV_VALUE'].sum() if 'PAYMENT_METHOD' in wpay_data.columns and 'TTV_VALUE' in wpay_data.columns else 0
    wpay_eftpos = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['GP_VALUE'].sum() if 'PAYMENT_METHOD' in wpay_data.columns else 0
    wpay_eftpos_ttv = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['TTV_VALUE'].sum() if 'PAYMENT_METHOD' in wpay_data.columns and 'TTV_VALUE' in wpay_data.columns else 0
    wpay_rest = wpay_total - wpay_amex - wpay_eftpos
    wpay_rest_ttv = wpay_total_ttv - wpay_amex_ttv - wpay_eftpos_ttv
    wpay_split = {ct: 0 for ct in payment_method_order}
    wpay_split_ttv = {ct: 0 for ct in payment_method_order}
    wpay_split['AMEX'] = wpay_amex
    wpay_split['EFTPOS'] = wpay_eftpos
    wpay_split_ttv['AMEX'] = wpay_amex_ttv
    wpay_split_ttv['EFTPOS'] = wpay_eftpos_ttv
    for ct, w in zip(other_card_types, other_weights):
        wpay_split[ct] = wpay_rest * (w / sum_weights) if wpay_rest > 0 else 0
        wpay_split_ttv[ct] = wpay_rest_ttv * (w / sum_weights) if wpay_rest_ttv > 0 else 0
    # --- Sum all acquirers ---
    for card_type in payment_method_order:
        adyen_managed = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['GP_VALUE'].sum()
        adyen_managed_ttv = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['TTV_VALUE'].sum() if 'TTV_VALUE' in bu_data.columns else 0
        adyen_balance = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['GP_VALUE'].sum()
        adyen_balance_ttv = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['TTV_VALUE'].sum() if 'TTV_VALUE' in bu_data.columns else 0
        wpay_value = wpay_split[card_type]
        wpay_value_ttv = wpay_split_ttv[card_type]
        gp_value = adyen_managed + adyen_balance + wpay_value
        ttv_value = adyen_managed_ttv + adyen_balance_ttv + wpay_value_ttv
        if show_bips and ttv_value > 0:
            value = (gp_value / ttv_value) * 10000
        else:
            value = gp_value
        card_type_values[card_type] = value
        total_gp += gp_value
        total_ttv += ttv_value
    if show_bips and total_ttv > 0:
        row['Total'] = (total_gp / total_ttv) * 10000
    else:
        row['Total'] = total_gp
    row.update(card_type_values)
    grand_total += row['Total']
    rows.append(row)

total_row = {'Business Unit': 'Total'}
total_sum = 0
for card_type in payment_method_order:
    if acquirer_filter == ["Wpay"]:
        pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
        gp = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['GP_VALUE'].sum() * pct
        ttv = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['TTV_VALUE'].sum() * pct if 'TTV_VALUE' in data.columns else 0
    elif acquirer_filter == ["Adyen Managed"]:
        gp = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['GP_VALUE'].sum()
        ttv = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['TTV_VALUE'].sum() if 'TTV_VALUE' in data.columns else 0
    elif acquirer_filter == ["Adyen Balance"]:
        gp = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['GP_VALUE'].sum()
        ttv = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['TTV_VALUE'].sum() if 'TTV_VALUE' in data.columns else 0
    else:
        adyen_managed_gp = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['GP_VALUE'].sum()
        adyen_managed_ttv = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['TTV_VALUE'].sum() if 'TTV_VALUE' in data.columns else 0
        adyen_balance_gp = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['GP_VALUE'].sum()
        adyen_balance_ttv = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['TTV_VALUE'].sum() if 'TTV_VALUE' in data.columns else 0
        wpay_total_gp = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['GP_VALUE'].sum()
        wpay_total_ttv = data[data['PAYMENT_METHOD'].str.contains('wpay', case=False)]['TTV_VALUE'].sum() if 'TTV_VALUE' in data.columns else 0
        wpay_pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
        wpay_gp = wpay_total_gp * wpay_pct
        wpay_ttv = wpay_total_ttv * wpay_pct
        gp = adyen_managed_gp + adyen_balance_gp + wpay_gp
        ttv = adyen_managed_ttv + adyen_balance_ttv + wpay_ttv
    if show_bips and ttv > 0:
        value = (gp / ttv) * 10000
    else:
        value = gp
    total_row[card_type] = value
    total_sum += value
total_row['Total'] = total_sum
rows.append(total_row)

# 1. Build value_rows_dollar from the original calculation (before any BIPS logic)
value_rows_dollar = []
for bu in data['Business Unit'].unique():
    bu_data = data[data['Business Unit'] == bu]
    value_row = {'Business Unit': bu}
    ttv_row = {}
    total_gp = 0
    total_ttv = 0
    for card_type in payment_method_order:
        gp = bu_data[bu_data['Card Type'] == card_type]['GP_VALUE'].sum()
        ttv = bu_data[bu_data['Card Type'] == card_type]['TTV_VALUE'].sum() if 'TTV_VALUE' in bu_data.columns else 0
        value_row[card_type] = gp
        ttv_row[card_type] = ttv
        total_gp += gp
        total_ttv += ttv
    value_row['Total'] = total_gp
    ttv_row['Total'] = total_ttv
    value_row['TTV'] = ttv_row  # Store TTVs for BIPS calculation
    value_rows_dollar.append(value_row)

# 2. Build percent_rows and percent_of_total_col from value_rows_dollar
percent_rows = []
percent_of_total_col = {}
total_gp_sum = sum(row['Total'] for row in value_rows_dollar)
for value_row in value_rows_dollar:
    pct_row = {'Business Unit': '% of GP'}
    for card_type in payment_method_order:
        pct = (value_row[card_type] / value_row['Total'] * 100) if value_row['Total'] > 0 else 0
        pct_row[card_type] = f"{pct:.2f}%"
    pct_row['Total'] = "100.00%"
    percent_rows.append(pct_row)
    percent_of_total_col[value_row['Business Unit']] = f"{(value_row['Total'] / total_gp_sum * 100):.2f}%" if total_gp_sum > 0 else "0.00%"

# 3. Build the final_rows for display
final_rows = []
for i, value_row in enumerate(value_rows_dollar):
    # Value row (dollar or BIPS)
    display_row = {'Business Unit': value_row['Business Unit']}
    for card_type in payment_method_order:
        if show_bips:
            ttv = value_row['TTV'][card_type]
            gp = value_row[card_type]
            bips = (gp / ttv * 10000) if ttv > 0 else 0
            display_row[card_type] = f"{bips:.2f}"
        else:
            display_row[card_type] = format_currency(value_row[card_type])
    if show_bips:
        ttv_total = value_row['TTV']['Total']
        gp_total = value_row['Total']
        bips_total = (gp_total / ttv_total * 10000) if ttv_total > 0 else 0
        display_row['Total'] = f"{bips_total:.2f}"
    else:
        display_row['Total'] = format_currency(value_row['Total'])
    display_row['% Of Total'] = percent_of_total_col[value_row['Business Unit']]
    final_rows.append(display_row)
    # Percent row (always from dollar values)
    final_rows.append(percent_rows[i])

# 4. Add the total and percent-of-total row, always from value_rows_dollar
total_dollar_row = {'Business Unit': 'Total'}
for card_type in payment_method_order:
    total = sum(row[card_type] for row in value_rows_dollar)
    ttv_total = sum(row['TTV'][card_type] for row in value_rows_dollar)
    if show_bips:
        bips = (total / ttv_total * 10000) if ttv_total > 0 else 0
        total_dollar_row[card_type] = f"{bips:.2f}"
    else:
        total_dollar_row[card_type] = format_currency(total)
if show_bips:
    gp_grand_total = sum(row['Total'] for row in value_rows_dollar)
    ttv_grand_total = sum(row['TTV']['Total'] for row in value_rows_dollar)
    bips_grand_total = (gp_grand_total / ttv_grand_total * 10000) if ttv_grand_total > 0 else 0
    total_dollar_row['Total'] = f"{bips_grand_total:.2f}"
else:
    total_dollar_row['Total'] = format_currency(sum(row['Total'] for row in value_rows_dollar))
total_dollar_row['% Of Total'] = "100.00%"
final_rows.append(total_dollar_row)

# Percent of total row, always from value_rows_dollar
total_pct_row = {'Business Unit': '% of Total'}
grand_total = sum(row['Total'] for row in value_rows_dollar)
for card_type in payment_method_order:
    total = sum(row[card_type] for row in value_rows_dollar)
    pct = (total / grand_total * 100) if grand_total > 0 else 0
    total_pct_row[card_type] = f"{pct:.2f}%"
total_pct_row['Total'] = "100.00%"
total_pct_row['% Of Total'] = ""
final_rows.append(total_pct_row)

final_df = pd.DataFrame(final_rows)

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
    # fit_columns_on_grid_load=True,  # Removed for content-width only
    # use_container_width=True,       # Removed for content-width only
)

# Add small italic note below the table and above the summary
st.markdown('<div style="text-align:right; font-size:12px;"><i>Note: Wpay card type distributions are based on market assumptions, except for AMEX and EFTPOS, which use actual data.</i></div>', unsafe_allow_html=True)

st.markdown('''
    <style>
    .main .block-container {
        padding-top: 1rem !important;
    }
    </style>
''', unsafe_allow_html=True)
