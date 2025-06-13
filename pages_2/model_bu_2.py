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

# Acquirer display mapping (if needed)
acquirer_display_map = {
    'adyen_managed': 'Adyen Managed',
    'adyen_balance': 'Adyen Balance',
    'wpay': 'Wpay'
}
acquirer_display_reverse = {v: k for k, v in acquirer_display_map.items()}

# Use the same get_filter_options as other pages
business_units, acquirers, months = get_filter_options()
default_month = get_previous_month()
months_desc = sorted(months, reverse=True)
all_bu = ["All"] + business_units
all_acquirer = ["All"] + [acquirer_display_map.get(a, a) for a in acquirers]
all_months = ["All"] + months_desc

# Updated filter layout to match other pages
col1, col2, col3 = st.columns(3)
with col1:
    selected_bu = st.multiselect("Business Unit", all_bu, default=["All"], key="model_bu_2")
with col2:
    selected_acquirer = st.multiselect("Acquirer", all_acquirer, default=["All"], key="model_acquirer_2")
with col3:
    selected_months = st.multiselect("Trading Month", all_months, default=[default_month], key="model_month_2")

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
        SUM(TTV) as TTV,
        SUM(ABS(MSF)) as MSF,
        SUM(ABS(ACQUIRER_FEE)) as COA,
        SUM(GP) as GP
    FROM dia_db.public.rba_model_data
    WHERE {where_clause}
    GROUP BY 1, 2, 3, 4
    ORDER BY 1, 2
    '''
    df = pd.read_sql(query, conn)
    # --- Wpay split logic (match TTV page) ---
    payment_method_order = [
        'AMEX', 'EFTPOS', 'Dom.DR', 'Dom.CR', 'Prem.DR', 'Prem.CR', 'Int.DR', 'Int.CR'
    ]
    other_card_types = ['Dom.DR', 'Dom.CR', 'Prem.CR', 'Prem.DR', 'Int.CR', 'Int.DR']
    other_weights = [25, 18.5, 10, 9.52, 3, 0.5]
    sum_weights = sum(other_weights)
    wpay_rows = []
    for bu in df['Business Unit'].unique():
        wpay_data = df[(df['Business Unit'] == bu) & (df['ACQUIRER'].str.contains('wpay', case=False, na=False))]
        if not wpay_data.empty:
            ttv_total = wpay_data['TTV'].sum()
            msf_total = wpay_data['MSF'].sum()
            coa_total = wpay_data['COA'].sum()
            gp_total = wpay_data['GP'].sum()
            ttv_amex = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['TTV'].sum()
            msf_amex = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['MSF'].sum()
            coa_amex = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['COA'].sum()
            gp_amex = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']['GP'].sum()
            ttv_eftpos = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['TTV'].sum()
            msf_eftpos = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['MSF'].sum()
            coa_eftpos = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['COA'].sum()
            gp_eftpos = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']['GP'].sum()
            ttv_rest = ttv_total - ttv_amex - ttv_eftpos
            msf_rest = msf_total - msf_amex - msf_eftpos
            coa_rest = coa_total - coa_amex - coa_eftpos
            gp_rest = gp_total - gp_amex - gp_eftpos
            # AMEX
            wpay_rows.append({'Business Unit': bu, 'Card Type': 'AMEX', 'TTV': ttv_amex, 'MSF': msf_amex, 'COA': coa_amex, 'GP': gp_amex})
            # EFTPOS
            wpay_rows.append({'Business Unit': bu, 'Card Type': 'EFTPOS', 'TTV': ttv_eftpos, 'MSF': msf_eftpos, 'COA': coa_eftpos, 'GP': gp_eftpos})
            # Other 6 types by weights
            for ct, w in zip(other_card_types, other_weights):
                wpay_rows.append({
                    'Business Unit': bu,
                    'Card Type': ct,
                    'TTV': ttv_rest * (w / sum_weights) if ttv_rest > 0 else 0,
                    'MSF': msf_rest * (w / sum_weights) if msf_rest > 0 else 0,
                    'COA': coa_rest * (w / sum_weights) if coa_rest > 0 else 0,
                    'GP': gp_rest * (w / sum_weights) if gp_rest > 0 else 0
                })
    wpay_df = pd.DataFrame(wpay_rows)
    if not wpay_df.empty:
        wpay_df = wpay_df.groupby(['Business Unit', 'Card Type'], as_index=False).sum(numeric_only=True)
        df = df[~df['ACQUIRER'].str.contains('wpay', case=False, na=False)]
        df = df.groupby(['Business Unit', 'Card Type'], as_index=False).sum(numeric_only=True)
        df = pd.concat([df, wpay_df], ignore_index=True)
        df = df.groupby(['Business Unit', 'Card Type'], as_index=False).sum(numeric_only=True)
    else:
        df = df.groupby(['Business Unit', 'Card Type'], as_index=False).sum(numeric_only=True)
    # Remove rows where all metrics are zero or NaN
    metric_cols = ['TTV', 'MSF', 'COA', 'GP']
    df = df[~((df[metric_cols].fillna(0) == 0).all(axis=1))]
    # Only include rows where Card Type is in payment_method_order
    df = df[df['Card Type'].isin(payment_method_order)]
    df['Card Type'] = pd.Categorical(df['Card Type'], categories=payment_method_order, ordered=True)
    df = df.sort_values(['Business Unit', 'Card Type'])
    return df

# Get the processed DataFrame with all columns (including assumption columns)
data = get_metric_data(bu_filter, acquirer_filter, month_filter)

# After getting the filtered DataFrame, add derived columns and assumption columns
# data = get_metric_data(bu_filter, acquirer_filter, month_filter) already exists above

data['MSF ex gst'] = data['MSF'] / 1.1

data['COA ex gst'] = data['COA'] / 1.1

data['GP ex gst'] = data['GP'] / 1.1

# Calculate base percent columns (percent of business unit)
def calc_base_percents(df, value_col, percent_col):
    for bu in df['Business Unit'].unique():
        bu_mask = (df['Business Unit'] == bu)
        bu_total = df.loc[bu_mask, value_col].sum()
        df.loc[bu_mask, percent_col] = df.loc[bu_mask, value_col] / bu_total * 100 if bu_total else 0
    return df

for metric, pct in zip(
    ['TTV', 'MSF ex gst', 'COA ex gst', 'GP ex gst'],
    ['% of TTV', '% of MSF', '% of COA', '% of GP']
):
    data = calc_base_percents(data, metric, pct)

# Add assumption columns, initialized to base values/%
for metric, pct in zip(
    ['TTV', 'MSF ex gst', 'COA ex gst', 'GP ex gst'],
    ['% of TTV', '% of MSF', '% of COA', '% of GP']
):
    data[f'{metric} (Assump)'] = data[metric]
    data[f'{pct} (Assump)'] = data[pct]

# Only include card type rows for each business unit (no manual summary rows)
display_df = data[data['Card Type'] != ''].copy()

# Add total row (sum of all business unit card type rows)
value_cols = ['TTV', 'TTV (Assump)', 'MSF ex gst', 'MSF ex gst (Assump)', 'COA ex gst', 'COA ex gst (Assump)', 'GP ex gst', 'GP ex gst (Assump)']
percent_cols = {
    '% of TTV': 'TTV', '% of TTV (Assump)': 'TTV (Assump)',
    '% of MSF': 'MSF ex gst', '% of MSF (Assump)': 'MSF ex gst (Assump)',
    '% of COA': 'COA ex gst', '% of COA (Assump)': 'COA ex gst (Assump)',
    '% of GP': 'GP ex gst', '% of GP (Assump)': 'GP ex gst (Assump)'
}
total_row = {col: display_df[col].sum() if col in value_cols else '' for col in display_df.columns}
total_row['Business Unit'] = 'Total'
total_row['Card Type'] = ''
for pct_col in percent_cols.keys():
    total_row[pct_col] = 100.0
display_df = pd.concat([display_df, pd.DataFrame([total_row])], ignore_index=True)

# Define display_columns before using it in AgGrid
display_columns = [
    'Business Unit', 'Card Type',
    'TTV', '% of TTV', 'TTV (Assump)', '% of TTV (Assump)',
    'MSF ex gst', '% of MSF', 'MSF ex gst (Assump)', '% of MSF (Assump)',
    'COA ex gst', '% of COA', 'COA ex gst (Assump)', '% of COA (Assump)',
    'GP ex gst', '% of GP', 'GP ex gst (Assump)', '% of GP (Assump)'
]

# Define column_defs before aggrid_options
column_defs = [
    {"field": "Business Unit", "headerName": "Business Unit", "rowGroup": True, "hide": True, "resizable": True, "pinned": "left", "width": 200},
    {"field": "Card Type", "headerName": "Card Type", "resizable": True, "width": 150},
    {"field": "TTV", "headerName": "TTV", "aggFunc": "sum", "cellStyle": {"backgroundColor": "#fff", "text-align": "right"}, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "resizable": True, "width": 140, "editable": False},
    {"field": "% of TTV", "headerName": "% of TTV", "cellStyle": {"backgroundColor": "#fff", "text-align": "right"}, "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'", "resizable": True, "width": 140, "editable": False},
    {"field": "TTV (Assump)", "headerName": "TTV", "aggFunc": "sum", "cellStyle": {"backgroundColor": "#e3f2fd", "text-align": "right"}, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "resizable": True, "width": 140, "editable": False},
    {"field": "% of TTV (Assump)", "headerName": "% of TTV", "cellStyle": {"backgroundColor": "#e3f2fd", "text-align": "right"}, "editable": True, "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'", "resizable": True, "width": 140},
    {"field": "MSF ex gst", "headerName": "MSF", "aggFunc": "sum", "cellStyle": {"backgroundColor": "#fff", "text-align": "right"}, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "resizable": True, "width": 140, "editable": False},
    {"field": "% of MSF", "headerName": "% of MSF", "cellStyle": {"backgroundColor": "#fff", "text-align": "right"}, "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'", "resizable": True, "width": 140, "editable": False},
    {"field": "MSF ex gst (Assump)", "headerName": "MSF", "aggFunc": "sum", "cellStyle": {"backgroundColor": "#e3f2fd", "text-align": "right"}, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "resizable": True, "width": 140, "editable": False},
    {"field": "% of MSF (Assump)", "headerName": "% of MSF", "cellStyle": {"backgroundColor": "#e3f2fd", "text-align": "right"}, "editable": True, "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'", "resizable": True, "width": 140},
    {"field": "COA ex gst", "headerName": "COA", "aggFunc": "sum", "cellStyle": {"backgroundColor": "#fff", "text-align": "right"}, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "resizable": True, "width": 140, "editable": False},
    {"field": "% of COA", "headerName": "% of COA", "cellStyle": {"backgroundColor": "#fff", "text-align": "right"}, "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'", "resizable": True, "width": 140, "editable": False},
    {"field": "COA ex gst (Assump)", "headerName": "COA", "aggFunc": "sum", "cellStyle": {"backgroundColor": "#e3f2fd", "text-align": "right"}, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "resizable": True, "width": 140, "editable": False},
    {"field": "% of COA (Assump)", "headerName": "% of COA", "cellStyle": {"backgroundColor": "#e3f2fd", "text-align": "right"}, "editable": True, "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'", "resizable": True, "width": 140},
    {"field": "GP ex gst", "headerName": "GP", "aggFunc": "sum", "cellStyle": {"backgroundColor": "#fff", "text-align": "right"}, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "resizable": True, "width": 140, "editable": False},
    {"field": "% of GP", "headerName": "% of GP", "cellStyle": {"backgroundColor": "#fff", "text-align": "right"}, "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'", "resizable": True, "width": 140, "editable": False},
    {"field": "GP ex gst (Assump)", "headerName": "GP", "aggFunc": "sum", "cellStyle": {"backgroundColor": "#e3f2fd", "text-align": "right"}, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "resizable": True, "width": 140, "editable": False},
    {"field": "% of GP (Assump)", "headerName": "% of GP", "cellStyle": {"backgroundColor": "#e3f2fd", "text-align": "right"}, "editable": True, "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'", "resizable": True, "width": 140},
]

# Define aggrid_options before using it in AgGrid
aggrid_options = {
    "columnDefs": column_defs,
    "defaultColDef": {"resizable": True, "minWidth": 60, "maxWidth": 300},
    "groupIncludeFooter": True,
    "suppressAggFuncInHeader": True,
    "groupDisplayType": 'singleColumn',
    "groupDefaultExpanded": 0,
    "groupColumnDef": {
        'headerName': 'Business Unit',
        'minWidth': 100,
        'maxWidth': 200,
        'cellStyle': {'fontWeight': 'bold'}
    },
    "domLayout": "normal",
    "autoHeaderHeight": True,
}

# Add custom CSS for even smaller font and more compact table, and wrap header text
st.markdown('''
    <style>
    .ag-theme-material .ag-cell {
        padding: 0px 2px !important;
        font-size: 8px !important;
        line-height: 1.2 !important;
    }
    .ag-theme-material .ag-header-cell {
        padding: 0px 2px !important;
        font-size: 8px !important;
        line-height: 1.2 !important;
    }
    .ag-theme-material .ag-header-cell-label {
        white-space: normal !important;
        line-height: 1.2 !important;
        height: auto !important;
    }
    .ag-theme-material .ag-header-cell-text {
        white-space: normal !important;
        line-height: 1.2 !important;
    }
    .ag-theme-material .ag-cell-value {
        line-height: 1.2 !important;
    }
    .aggrid-scroll-x {
        overflow-x: auto;
        width: 100%;
    }
    </style>
''', unsafe_allow_html=True)

# Store original business unit totals for each metric
original_totals = {}
for bu in display_df['Business Unit'].dropna().unique():
    group_row = display_df[(display_df['Business Unit'] == bu) & (display_df['Card Type'] == '')]
    if not group_row.empty:
        original_totals[bu] = {
            'TTV': group_row['TTV'].values[0],
            'MSF ex gst': group_row['MSF ex gst'].values[0],
            'COA ex gst': group_row['COA ex gst'].values[0],
            'GP ex gst': group_row['GP ex gst'].values[0],
        }

# On first load, initialize session state
if 'model_bu_2_edited' not in st.session_state:
    st.session_state['model_bu_2_edited'] = display_df.copy()

# Always use session state DataFrame for the grid
edited_df = st.session_state['model_bu_2_edited']

# Dummy widget to force rerun
st.text_input("Force rerun (type anything here to force update):", key="force_rerun")

# Show the grid and get edited data
st.markdown('<div class="aggrid-scroll-x" style="width:2600px; overflow-x:auto;">', unsafe_allow_html=True)
grid_response = AgGrid(
    edited_df[display_columns],
    gridOptions=aggrid_options,
    enable_enterprise_modules=True,
    update_mode='VALUE_CHANGED',  # triggers on every cell edit
    theme='material',
    height=600,
    allow_unsafe_jscode=True,
    reload_data=True,
    editable=True,
    data_return_mode='AS_INPUT',
    fit_columns_on_grid_load=False,
)
st.markdown('</div>', unsafe_allow_html=True)
edited_df = grid_response['data']

# Debug: print columns and percent values
st.write("Columns in edited_df:", edited_df.columns.tolist())
st.write("Columns (repr):", [repr(col) for col in edited_df.columns])
st.write("Edited % of TTV (Assump):", edited_df['% of TTV (Assump)'] if '% of TTV (Assump)' in edited_df.columns else 'Column not found')

# Ensure numeric columns for calculations
for col in [
    '% of TTV (Assump)', '% of MSF (Assump)', '% of COA (Assump)', '% of GP (Assump)',
    'TTV', 'MSF ex gst', 'COA ex gst', 'GP ex gst'
]:
    if col in edited_df.columns:
        edited_df[col] = pd.to_numeric(edited_df[col], errors='coerce')

# Recalculate assumption value columns after every edit
metrics = [
    ('TTV', 'TTV (Assump)', '% of TTV (Assump)'),
    ('MSF ex gst', 'MSF ex gst (Assump)', '% of MSF (Assump)'),
    ('COA ex gst', 'COA ex gst (Assump)', '% of COA (Assump)'),
    ('GP ex gst', 'GP ex gst (Assump)', '% of GP (Assump)'),
]
for bu in edited_df['Business Unit'].dropna().unique():
    for metric, metric_assump, pct_assump in metrics:
        if bu in original_totals:
            total = original_totals[bu][metric]
            mask = (edited_df['Business Unit'] == bu) & (edited_df['Card Type'] != '')
            st.write(f"BU: {bu}, metric: {metric}, total: {total}")
            st.write("Mask sum:", mask.sum())
            st.write("Before:", edited_df.loc[mask, metric_assump])
            st.write("Percent used:", edited_df.loc[mask, pct_assump])
            edited_df.loc[mask, metric_assump] = (
                edited_df.loc[mask, pct_assump] / 100 * total
            )
            st.write("After:", edited_df.loc[mask, metric_assump])

# Debug: print recalculated values
st.write("Recalculated TTV (Assump):", edited_df['TTV (Assump)'] if 'TTV (Assump)' in edited_df.columns else 'Column not found')

# Save edits and recalculated values to session state
st.session_state['model_bu_2_edited'] = edited_df.copy()

# Update valueFormatter for all '% of ... (Assump)' columns to always show 2 decimals
for col in column_defs:
    if col['field'].endswith('(Assump)') and '% of' in col['headerName']:
        col['valueFormatter'] = "x == null ? '' : Number(x).toFixed(2) + '%'"

# Add custom CSS for even smaller font and more compact table
st.markdown('''
    <style>
    .ag-theme-material .ag-cell {
        padding: 0px 2px !important;
        font-size: 8px !important;
        line-height: 1.2 !important;
    }
    .ag-theme-material .ag-header-cell {
        padding: 0px 2px !important;
        font-size: 8px !important;
        line-height: 1.2 !important;
    }
    .ag-theme-material .ag-header-cell-text {
        white-space: normal !important;
        line-height: 1.2 !important;
    }
    .ag-theme-material .ag-cell-value {
        line-height: 1.2 !important;
    }
    @media (max-width: 700px) {
        .ag-theme-material .ag-cell, .ag-theme-material .ag-header-cell {
            font-size: 7px !important;
            padding: 0px 1px !important;
        }
    }
    </style>
''', unsafe_allow_html=True) 