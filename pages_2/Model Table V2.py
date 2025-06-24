import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from config import SNOWFLAKE_CONFIG
import numpy as np
from st_aggrid.shared import JsCode

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
    df = pd.read_sql("SELECT DISTINCT SOURCE, DISPLAY_NAME, ACQUIRER, TRADING_MONTH, ACCOUNT_MANAGER FROM dia_db.public.rba_model_data", conn)
    business_units = sorted(df['SOURCE'].dropna().unique().tolist())
    merchants = sorted(df['DISPLAY_NAME'].dropna().unique().tolist())
    acquirers = sorted(df['ACQUIRER'].dropna().unique().tolist())
    months = sorted(df['TRADING_MONTH'].dropna().unique().tolist())
    account_managers = sorted(df['ACCOUNT_MANAGER'].dropna().unique().tolist())
    return business_units, merchants, acquirers, months, account_managers

st.markdown(f'''
    <style>
    .main .block-container {{
        padding-top: 1rem !important;
    }}
    </style>
''', unsafe_allow_html=True)

st.subheader("Business Unit Model Summary")

# Add instruction text with new design (above filters only)
st.markdown('''
<div style="background-color:#e3f2fd; border-radius:8px; padding:18px 18px 10px 18px; margin-bottom:18px;">
<h4 style="margin-top:0;">Instruction on Model Table</h4>
<ul style="font-size:16px;">
  <li><b>Base values</b> and <span style="color:#1976d2;">assumption columns</span> are shown in the table below.</li>
  <li><b>GP (base and assumption) is always calculated as MSF - COA (ex GST).</b></li>
  <li><b>Blue background</b> = editable assumption columns. Click to adjust values.</li>
  <li>Edit % values as needed, then click <b>Update Model Table</b> to apply your changes.</li>
  <li>When you change TTV %, only TTV assumption values update. MSF and COA Bips remain unchanged and use baseline TTV data.</li>
  <li>When you change MSF or COA Bips, GP updates automatically.</li>
</ul>
</div>
''', unsafe_allow_html=True)

# Acquirer display mapping (if needed)
acquirer_display_map = {
    'adyen_managed': 'Adyen Managed',
    'adyen_balance': 'Adyen Balance',
    'wpay': 'Wpay'
}
acquirer_display_reverse = {v: k for k, v in acquirer_display_map.items()}

# Use the same get_filter_options as other pages
business_units, merchants, acquirers, months, account_managers = get_filter_options()
default_month = get_previous_month()
months_desc = sorted(months, reverse=True)
all_bu = ["All"] + business_units
all_merchants = ["All"] + merchants
all_acquirer = ["All"] + [acquirer_display_map.get(a, a) for a in acquirers]
all_months = ["All"] + months_desc

# Updated filter layout to match other pages
col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    selected_bu = st.multiselect("Business Unit", all_bu, default=["All"], key="model_bu_2")
with col2:
    selected_acquirer = st.multiselect("Acquirer", all_acquirer, default=["All"], key="model_acquirer_2")
with col3:
    selected_merchants = st.multiselect("Merchant Name", all_merchants, default=["All"], key="model_merchant_2")
with col4:
    selected_months = st.multiselect("Trading Month", all_months, default=[default_month], key="model_month_2")
with col5:
    selected_account_manager = st.multiselect("Account Manager", ["All"] + account_managers, default=["All"], key="model_account_manager_2")

selected_acquirer_internal = [acquirer_display_reverse.get(a, a) for a in selected_acquirer]

def get_selected_or_all(selected, all_values):
    if "All" in selected or not selected:
        return all_values
    return selected

bu_filter = get_selected_or_all(selected_bu, business_units)
merchant_filter = get_selected_or_all(selected_merchants, merchants)
acquirer_filter = get_selected_or_all(selected_acquirer_internal, acquirers)
month_filter = get_selected_or_all(selected_months, months_desc)
account_manager_filter = get_selected_or_all(selected_account_manager, account_managers)

@st.cache_data
def get_metric_data(bu_list, merchant_list, acquirer_list, month_list, account_manager_list):
    conn = init_snowflake_connection()
    where_clauses = []
    if bu_list and len(bu_list) < len(business_units):
        bu_str = ", ".join([f"'{b}'" for b in bu_list])
        where_clauses.append(f"SOURCE IN ({bu_str})")
    if merchant_list and len(merchant_list) < len(merchants):
        # Properly escape single quotes in merchant names for the SQL query
        escaped_merchants = [m.replace("'", "''") for m in merchant_list]
        merchant_str = ", ".join([f"'{m}'" for m in escaped_merchants])
        where_clauses.append(f"DISPLAY_NAME IN ({merchant_str})")
    if acquirer_list and len(acquirer_list) < len(acquirers):
        acq_str = ", ".join([f"'{a}'" for a in acquirer_list])
        where_clauses.append(f"LOWER(ACQUIRER) IN ({acq_str})")
    if month_list and len(month_list) < len(months_desc):
        month_str = ", ".join([f"'{m}'" for m in month_list])
        where_clauses.append(f"TRADING_MONTH IN ({month_str})")
    if account_manager_list and len(account_manager_list) < len(account_managers):
        am_str = ", ".join([f"'{a}'" for a in account_manager_list])
        where_clauses.append(f"ACCOUNT_MANAGER IN ({am_str})")
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
            DISPLAY_NAME as "Merchant",
            CASE 
                WHEN payment_method_variant IN ('amex','amex_applepay','amex_googlepay') THEN 'AMEX'
                WHEN payment_method_variant IN ('eftpos_australia','eftpos_australia_chq','eftpos_australia_sav') THEN 'EFTPOS'
                WHEN payment_method_variant IN ('visa','mcstandardcredit','mccredit','visastandardcredit','mc') THEN 'Dom.CR'
                WHEN payment_method_variant IN ('visaprepaidanonymous','visastandarddebit','mcprepaidanonymous','mcdebit','maestro','mcstandarddebit', 'visadebit') THEN 'Dom.DR'
                WHEN payment_method_variant IN ('visapremiumcredit','mcpremiumcredit','visacorporatecredit','visacommercialpremiumcredit','mc_applepay','visacommercialsuperpremiumcredit','visa_applepay','mccorporatecredit','visa_googlepay','mccommercialcredit','mcfleetcredit','visabusiness','mcpurchasingcredit','visapurchasingcredit','mc_googlepay','visasuperpremiumcredit','visacommercialcredit','visafleetcredit','mcsuperpremiumcredit') THEN 'Prem.CR'
                WHEN payment_method_variant IN ('visasuperpremiumdebit','visapremiumdebit','mcsuperpremiumdebit','visacorporatedebit','mcpremiumdebit','visacommercialpremiumdebit','mccommercialdebit','mccorporatedebit','visacommercialdebit','visacommercialsuperpremiumdebit') THEN 'Prem.DR'
                WHEN payment_method_variant IN ('discover','jcbcredit','diners', 'alipay', 'cupcredit', 'cup') THEN 'Int.CR'
                WHEN payment_method_variant IN ('vpay','electron','jcbdebit','visadankort', 'cupdebit') THEN 'Int.DR'
                ELSE 'Other'
            END AS "Card Type",
            PAYMENT_METHOD,
            LOWER(ACQUIRER) as ACQUIRER,
            SUM(TTV) as TTV,
            SUM(MSF) as MSF,
            SUM(ACQUIRER_FEE) as COA,
            SUM(SURCHARGE_AMOUNT) as SURCHARGE
        FROM dia_db.public.rba_model_data
        WHERE {where_clause}
        GROUP BY 1, 2, 3, 4, 5
    )
    SELECT 
        "Business Unit",
        "Merchant",
        "Card Type",
        PAYMENT_METHOD,
        ACQUIRER,
        SUM(TTV) as TTV,
        SUM(MSF) as MSF,
        SUM(COA) as COA,
        SUM(SURCHARGE) as SURCHARGE
    FROM base_data
    GROUP BY 1, 2, 3, 4, 5
    ORDER BY "Business Unit", "Merchant", "Card Type"
    """
    df = pd.read_sql(query, conn)
    
    # Process data with proper hierarchy
    rows = []
    for bu in df['Business Unit'].unique():
        bu_data = df[df['Business Unit'] == bu]
        
        # --- Process "All" Merchant Group ---
        card_type_values = {}
        
        # Surcharge needs to be aggregated alongside other metrics
        wpay_data = bu_data[bu_data['ACQUIRER'].str.contains('wpay', case=False)]
        wpay_ttv_total = wpay_data['TTV'].sum()
        wpay_msf_total = wpay_data['MSF'].sum()
        wpay_coa_total = wpay_data['COA'].sum()
        wpay_surcharge_total = wpay_data['SURCHARGE'].sum()

        wpay_amex_data = wpay_data[wpay_data['PAYMENT_METHOD'] == 'AMEX']
        wpay_amex_ttv = wpay_amex_data['TTV'].sum()
        wpay_amex_msf = wpay_amex_data['MSF'].sum()
        wpay_amex_coa = wpay_amex_data['COA'].sum()
        wpay_amex_surcharge = wpay_amex_data['SURCHARGE'].sum()

        wpay_eftpos_data = wpay_data[wpay_data['PAYMENT_METHOD'] == 'EFTPOS']
        wpay_eftpos_ttv = wpay_eftpos_data['TTV'].sum()
        wpay_eftpos_msf = wpay_eftpos_data['MSF'].sum()
        wpay_eftpos_coa = wpay_eftpos_data['COA'].sum()
        wpay_eftpos_surcharge = wpay_eftpos_data['SURCHARGE'].sum()

        wpay_rest_ttv = wpay_ttv_total - wpay_amex_ttv - wpay_eftpos_ttv
        wpay_rest_msf = wpay_msf_total - wpay_amex_msf - wpay_eftpos_msf
        wpay_rest_coa = wpay_coa_total - wpay_amex_coa - wpay_eftpos_coa
        wpay_rest_surcharge = wpay_surcharge_total - wpay_amex_surcharge - wpay_eftpos_surcharge
        
        other_card_types = ['Dom.DR', 'Dom.CR', 'Prem.DR', 'Prem.CR', 'Int.DR', 'Int.CR']
        other_weights = [25, 18.5, 10, 9.52, 3, 0.5]
        sum_weights = sum(other_weights)
        
        for card_type in payment_method_order:
            adyen_managed_ttv = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['TTV'].sum()
            adyen_managed_msf = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['MSF'].sum()
            adyen_managed_coa = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['COA'].sum()
            adyen_managed_surcharge = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['SURCHARGE'].sum()

            adyen_balance_ttv = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['TTV'].sum()
            adyen_balance_msf = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['MSF'].sum()
            adyen_balance_coa = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['COA'].sum()
            adyen_balance_surcharge = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['SURCHARGE'].sum()
            
            if card_type == 'AMEX':
                wpay_ttv, wpay_msf, wpay_coa, wpay_surcharge = wpay_amex_ttv, wpay_amex_msf, wpay_amex_coa, wpay_amex_surcharge
            elif card_type == 'EFTPOS':
                wpay_ttv, wpay_msf, wpay_coa, wpay_surcharge = wpay_eftpos_ttv, wpay_eftpos_msf, wpay_eftpos_coa, wpay_eftpos_surcharge
            else:
                if card_type in other_card_types and sum_weights > 0:
                    weight_idx = other_card_types.index(card_type)
                    weight = other_weights[weight_idx] / sum_weights
                    wpay_ttv = wpay_rest_ttv * weight
                    wpay_msf = wpay_rest_msf * weight
                    wpay_coa = wpay_rest_coa * weight
                    wpay_surcharge = wpay_rest_surcharge * weight
                else:
                    wpay_ttv, wpay_msf, wpay_coa, wpay_surcharge = 0, 0, 0, 0
            
            total_ttv = adyen_managed_ttv + adyen_balance_ttv + wpay_ttv
            total_msf = adyen_managed_msf + adyen_balance_msf + wpay_msf
            total_coa = adyen_managed_coa + adyen_balance_coa + wpay_coa
            total_surcharge = adyen_managed_surcharge + adyen_balance_surcharge + wpay_surcharge
            
            card_type_values[card_type] = {'TTV': total_ttv, 'MSF': total_msf, 'COA': total_coa, 'SURCHARGE': total_surcharge}

        for card_type in payment_method_order:
            if card_type in card_type_values and card_type_values[card_type]['TTV'] > 0:
                row_data = {'Business Unit': bu, 'Merchant': 'All', 'Card Type': card_type, **card_type_values[card_type]}
                rows.append(row_data)
        
        # --- Process Individual Merchants ---
        merchant_list = sorted(bu_data['Merchant'].dropna().unique())
        for merchant in merchant_list:
            if pd.notna(merchant) and merchant != '':
                merchant_data = bu_data[bu_data['Merchant'] == merchant]
                
                for card_type in payment_method_order:
                    card_data = merchant_data[merchant_data['Card Type'] == card_type]
                    if not card_data.empty and card_data['TTV'].sum() > 0:
                         row_data = {
                            'Business Unit': bu, 'Merchant': merchant, 'Card Type': card_type,
                            'TTV': card_data['TTV'].sum(), 'MSF': card_data['MSF'].sum(), 'COA': card_data['COA'].sum(), 'SURCHARGE': card_data['SURCHARGE'].sum()
                         }
                         rows.append(row_data)

    result_df = pd.DataFrame(rows)
    if not result_df.empty:
        # First, fill NA for existing numeric columns that come from the initial query
        existing_numeric_cols = ['TTV', 'MSF', 'COA', 'SURCHARGE']
        for col in existing_numeric_cols:
            if col in result_df.columns:
                result_df[col] = result_df[col].fillna(0)
        
        # Now, calculate the 'GP' column
        result_df['GP'] = result_df['MSF'] - result_df['COA']

        final_rows = []
        metric_cols = ['TTV', 'MSF', 'COA', 'GP', 'SURCHARGE']

        for bu, group_df in result_df.groupby('Business Unit'):
            final_rows.extend(group_df.to_dict('records'))
            
            individual_merchants_df = group_df[group_df['Merchant'] != 'All']
            if not individual_merchants_df.empty:
                adjustment_values = individual_merchants_df[metric_cols].sum()
                
                adjustment_row = {'Business Unit': bu, 'Merchant': '__ADJUSTMENT__'}
                for col in metric_cols:
                    adjustment_row[col] = -adjustment_values[col]
                final_rows.append(adjustment_row)
        
        result_df = pd.DataFrame(final_rows)

        # Only fill numeric columns to avoid Categorical errors
        result_df[metric_cols] = result_df[metric_cols].fillna(0)
        result_df = result_df[~((result_df[metric_cols].fillna(0) == 0).all(axis=1))]
        result_df = result_df[(result_df['Card Type'].isin(payment_method_order)) | (result_df['Card Type'] == '') | (result_df['Merchant'] == '__ADJUSTMENT__')]
        result_df['sorter'] = np.where(result_df['Merchant'] == 'All', 0, 1)
        result_df['Card Type'] = pd.Categorical(result_df['Card Type'], categories=[''] + payment_method_order, ordered=True)
        result_df = result_df.sort_values(['Business Unit', 'sorter', 'Merchant', 'Card Type']).drop('sorter', axis=1)
    
    return result_df

@st.cache_data
def process_data(df):
    """Applies initial processing and calculations to the dataframe."""
    if df.empty:
        return df

    # --- Data Processing and BIPS Calculation ---
    # Only fill numeric columns to avoid Categorical errors.
    numeric_cols_to_fill = ['TTV', 'MSF', 'COA', 'GP', 'SURCHARGE']
    for col in numeric_cols_to_fill:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    df['MSF ex gst'] = df['MSF'] / 1.1
    df['COA ex gst'] = df['COA'] / 1.1
    df['GP ex gst'] = df['GP'] / 1.1
    df['MSF Bips'] = np.where(df['TTV'] > 0, (df['MSF ex gst'] / df['TTV']) * 10000, 0)
    df['COA Bips'] = np.where(df['TTV'] > 0, (df['COA ex gst'] / df['TTV']) * 10000, 0)
    df['GP Bips'] = np.where(df['TTV'] > 0, (df['GP ex gst'] / df['TTV']) * 10000, 0)

    # --- Base Percentage Calculation ---
    # Pre-calculate % of Total TTV for later aggregation
    total_ttv = df['TTV'].sum()
    df['% of TTV'] = (df['TTV'] / total_ttv * 100) if total_ttv else 0

    # --- Initialize Assumption Columns ---
    df['TTV (Assump)'] = df['TTV']
    df['% of TTV (Assump)'] = df['% of TTV']
    df['MSF ex gst (Assump)'] = df['MSF ex gst']
    df['MSF Bips (Assump)'] = df['MSF Bips']
    df['COA ex gst (Assump)'] = df['COA ex gst']
    df['COA Bips (Assump)'] = df['COA Bips']
    df['GP ex gst (Assump)'] = df['GP ex gst']
    df['GP Bips (Assump)'] = df['GP Bips']
    
    return df

def recalculate_data(df):
    """
    Recalculates all assumption values using a robust vectorized approach.
    This honors the user's specified calculation hierarchy and avoids grouping/indexing errors.
    """
    df_copy = df.copy()

    # Ensure correct data types to prevent calculation errors
    numeric_cols = [
        'TTV', '% of TTV', 'TTV (Assump)', '% of TTV (Assump)',
        'MSF ex gst', 'MSF Bips', 'MSF ex gst (Assump)', 'MSF Bips (Assump)',
        'COA ex gst', 'COA Bips', 'COA ex gst (Assump)', 'COA Bips (Assump)',
        'GP ex gst', 'GP Bips', 'GP ex gst (Assump)', 'GP Bips (Assump)'
    ]
    for col in numeric_cols:
        df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)

    # Step 1: Create a map of each BU to its correct total base TTV.
    # This sums TTV from only the card type detail rows within the 'All' merchant group.
    card_type_rows = df_copy[(df_copy['Merchant'] == 'All') & (df_copy['Card Type'].isin(payment_method_order))]
    bu_total_ttv_map = card_type_rows.groupby('Business Unit')['TTV'].sum()
    df_copy['bu_total_base_ttv'] = df_copy['Business Unit'].map(bu_total_ttv_map)

    # Step 2: Identify ONLY the card type rows to perform calculations on.
    mask = (df_copy['Merchant'] == 'All') & (df_copy['Card Type'].isin(payment_method_order))

    # Step 3: Apply calculations using the mask for precision.
    # a. Recalculate TTV (Assump)
    df_copy.loc[mask, 'TTV (Assump)'] = \
        (df_copy.loc[mask, '% of TTV (Assump)'] / 100) * df_copy.loc[mask, 'bu_total_base_ttv']

    # b. Recalculate MSF/COA (Assump) from BASE TTV and their Bips
    df_copy.loc[mask, 'MSF ex gst (Assump)'] = \
        df_copy.loc[mask, 'TTV'] * df_copy.loc[mask, 'MSF Bips (Assump)'] / 10000
    df_copy.loc[mask, 'COA ex gst (Assump)'] = \
        df_copy.loc[mask, 'TTV'] * df_copy.loc[mask, 'COA Bips (Assump)'] / 10000

    # c. Recalculate GP (Assump)
    df_copy.loc[mask, 'GP ex gst (Assump)'] = \
        df_copy.loc[mask, 'MSF ex gst (Assump)'] - df_copy.loc[mask, 'COA ex gst (Assump)']
    
    # d. Recalculate GP Bips (Assump)
    ttv_assump = df_copy.loc[mask, 'TTV (Assump)']
    gp_assump = df_copy.loc[mask, 'GP ex gst (Assump)']
    df_copy.loc[mask, 'GP Bips (Assump)'] = \
        np.where(ttv_assump > 0, (gp_assump / ttv_assump) * 10000, 0)

    # Step 4: Clean up.
    # Only fill numeric columns to avoid Categorical errors
    numeric_cols_cleanup = [
        'TTV', '% of TTV', 'TTV (Assump)', '% of TTV (Assump)',
        'MSF ex gst', 'MSF Bips', 'MSF ex gst (Assump)', 'MSF Bips (Assump)',
        'COA ex gst', 'COA Bips', 'COA ex gst (Assump)', 'COA Bips (Assump)',
        'GP ex gst', 'GP Bips', 'GP ex gst (Assump)', 'GP Bips (Assump)'
    ]
    for col in numeric_cols_cleanup:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna(0)
    return df_copy 

# --- Main data loading and caching ---
raw_data = get_metric_data(bu_filter, merchant_filter, acquirer_filter, month_filter, account_manager_filter)
processed_data = process_data(raw_data)

# --- State Management for Editable Grid ---
# Initialize a counter for forcing grid updates. This is the key to the fix.
if 'update_counter' not in st.session_state:
    st.session_state.update_counter = 0

grid_key_base = f"model_bu_2_grid_{'-'.join(bu_filter)}-{'-'.join(merchant_filter)}-{'-'.join(acquirer_filter)}-{'-'.join(month_filter)}-{'-'.join(account_manager_filter)}"
# The grid key now includes the counter, forcing it to re-render when the counter changes.
grid_key_with_counter = f"{grid_key_base}_{st.session_state.update_counter}"

if 'edited_data' not in st.session_state:
    st.session_state.edited_data = {}

# If filters change, reset the edited data for the new base key and reset the counter.
if grid_key_base not in st.session_state.edited_data:
    st.session_state.edited_data[grid_key_base] = processed_data.copy()
    st.session_state.update_counter = 0

df_for_grid = st.session_state.edited_data[grid_key_base]

st.markdown("<h5>Interactive Buttons</h5>", unsafe_allow_html=True)

# --- Buttons above the table ---
col1, col2, _ = st.columns([1, 1, 3])
with col1:
    update_button = st.button("Update Model Table", type="primary", help="Apply assumption changes to the model table")
with col2:
    surcharge_ban_button = st.button("Surcharge Ban on Debit Card", type="primary", help="Set MSF Bips for Debit cards to 65")

# --- Display Grid ---
total_row_data = None
if not df_for_grid.empty:
    total_row = {}
    all_rows_df = df_for_grid[(df_for_grid['Merchant'] == 'All') & (df_for_grid['Card Type'].isin(payment_method_order))]
    numeric_cols = ['TTV', 'TTV (Assump)', 'MSF ex gst', 'MSF ex gst (Assump)', 'COA ex gst', 'COA ex gst (Assump)', 'GP ex gst', 'GP ex gst (Assump)']
    for col in numeric_cols:
        total_row[col] = pd.to_numeric(all_rows_df[col], errors='coerce').sum()
    
    bips_cols = ['MSF Bips', 'MSF Bips (Assump)', 'COA Bips', 'COA Bips (Assump)', 'GP Bips', 'GP Bips (Assump)']
    for col in bips_cols:
        ttv_col = 'TTV (Assump)' if 'Assump' in col else 'TTV'
        total_ttv = total_row.get(ttv_col, 0)
        if total_ttv > 0:
            total_row[col] = (pd.to_numeric(all_rows_df[col], errors='coerce') * pd.to_numeric(all_rows_df[ttv_col], errors='coerce')).sum() / total_ttv
        else:
            total_row[col] = 0
            
    total_row['% of TTV'] = 100.0
    total_row['% of TTV (Assump)'] = 100.0
    total_row['Card Type'] = 'TOTAL'
    total_row_data = [total_row]

assump_cell_style_right = JsCode("""
    function(params) {
        if (params.node.isRowPinned()) {
            // Apply purple background for pinned (total) rows in assumption columns
            return {'background-color': '#5D3A9B', 'color': 'white', 'textAlign': 'right'};
        }
        // Keep the light blue background for regular data rows
        return {'backgroundColor': '#e3f2fd', 'textAlign': 'right'};
    }
""")

assump_cell_style_default = JsCode("""
    function(params) {
        if (params.node.isRowPinned()) {
            // Apply purple background for pinned (total) rows in assumption columns
            return {'background-color': '#5D3A9B', 'color': 'white'};
        }
        // Keep the light blue background for regular data rows
        return {'backgroundColor': '#e3f2fd'};
    }
""")

# JS to capture expanded group keys
get_expanded_js = JsCode("""
function(e) {
    let expanded = [];
    this.api.forEachNode(function(node) {
        if (node.expanded && node.group) {
            expanded.push(node.key);
        }
    });
    return {expanded: expanded};
}
""")

# JS to restore expanded group keys
restore_expanded_js = JsCode(f"""
function(params) {{
    let expanded = {st.session_state.get('expanded_keys', [])};
    this.api.forEachNode(function(node) {{
        if (node.group && expanded.includes(node.key)) {{
            node.setExpanded(true);
        }}
    }});
}}
""")

# Define grid options dictionary manually
grid_options = {
    "columnDefs": [
        {"field": "Business Unit", "rowGroup": True, "hide": True},
        {"field": "Merchant", "rowGroup": True, "hide": True},
        {"field": "Card Type", "headerName": "Card Type", "resizable": True, "width": 150, "pinned": "left", "headerClass": "purple-header"},
        # Add a hidden SURCHARGE column with a sum aggregation to make it available for calculations
        {"field": "SURCHARGE", "aggFunc": "sum", "hide": True},
        {"headerName": "TTV", "headerClass": "center-aligned-header", "children": [
            {"field": "TTV", "headerName": "Base", "aggFunc": "sum", "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": {"textAlign": "right"}},
            # Use sum aggregation on the pre-calculated % of TTV
            {"field": "% of TTV", "headerName": "%", "aggFunc": "sum", "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'"},
            {"field": "TTV (Assump)", "headerName": "Assump", "aggFunc": "sum", "editable": True, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": assump_cell_style_right},
            {"field": "% of TTV (Assump)", "headerName": "%", "aggFunc": "sum", "editable": True, "valueFormatter": "x == null ? '' : Number(x).toFixed(2) + '%'", "cellStyle": assump_cell_style_default}
        ]},
        {"headerName": "MSF", "headerClass": "center-aligned-header", "children": [
            {"field": "MSF ex gst", "headerName": "Base", "aggFunc": "sum", "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": {"textAlign": "right"}},
            # Add a valueGetter for custom BIPS calculation on group rows
            {"headerName": "Bips", "valueFormatter": "x == null ? '' : Number(x).toFixed(2)", "valueGetter": JsCode("""
                function(params) {
                    if (!params.node.group) { return params.data['MSF Bips']; }
                    const msf = params.node.aggData['MSF ex gst'];
                    const ttv = params.node.aggData['TTV'];
                    const surcharge = params.node.aggData['SURCHARGE'];
                    if ((ttv - surcharge) > 0) { return (msf / (ttv - surcharge)) * 10000; }
                    return 0;
                }
            """)},
            {"field": "MSF ex gst (Assump)", "headerName": "Assump", "aggFunc": "sum", "editable": True, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": assump_cell_style_right},
            {"headerName": "Bips", "editable": True, "valueFormatter": "x == null ? '' : Number(x).toFixed(2)", "cellStyle": assump_cell_style_default, "valueGetter": JsCode("""
                function(params) {
                    if (!params.node.group) { return params.data['MSF Bips (Assump)']; }
                    const msf = params.node.aggData['MSF ex gst (Assump)'];
                    const ttv = params.node.aggData['TTV (Assump)'];
                    const surcharge = params.node.aggData['SURCHARGE']; /* Surcharge is not an assumption */
                    if ((ttv - surcharge) > 0) { return (msf / (ttv - surcharge)) * 10000; }
                    return 0;
                }
            """)}
        ]},
        {"headerName": "COA", "headerClass": "center-aligned-header", "children": [
            {"field": "COA ex gst", "headerName": "Base", "aggFunc": "sum", "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": {"textAlign": "right"}},
            {"headerName": "Bips", "valueFormatter": "x == null ? '' : Number(x).toFixed(2)", "valueGetter": JsCode("""
                function(params) {
                    if (!params.node.group) { return params.data['COA Bips']; }
                    const coa = params.node.aggData['COA ex gst'];
                    const ttv = params.node.aggData['TTV'];
                    if (ttv > 0) { return (coa / ttv) * 10000; }
                    return 0;
                }
            """)},
            {"field": "COA ex gst (Assump)", "headerName": "Assump", "aggFunc": "sum", "editable": True, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": assump_cell_style_right},
            {"headerName": "Bips", "editable": True, "valueFormatter": "x == null ? '' : Number(x).toFixed(2)", "cellStyle": assump_cell_style_default, "valueGetter": JsCode("""
                function(params) {
                    if (!params.node.group) { return params.data['COA Bips (Assump)']; }
                    const coa = params.node.aggData['COA ex gst (Assump)'];
                    const ttv = params.node.aggData['TTV (Assump)'];
                    if (ttv > 0) { return (coa / ttv) * 10000; }
                    return 0;
                }
            """)}
        ]},
        {"headerName": "GP", "headerClass": "center-aligned-header", "children": [
            {"field": "GP ex gst", "headerName": "Base", "aggFunc": "sum", "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": {"textAlign": "right"}},
            {"headerName": "Bips", "valueFormatter": "x == null ? '' : Number(x).toFixed(2)", "valueGetter": JsCode("""
                function(params) {
                    if (!params.node.group) { return params.data['GP Bips']; }
                    const gp = params.node.aggData['GP ex gst'];
                    const ttv = params.node.aggData['TTV'];
                    if (ttv > 0) { return (gp / ttv) * 10000; }
                    return 0;
                }
            """)},
            {"field": "GP ex gst (Assump)", "headerName": "Assump", "aggFunc": "sum", "editable": True, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": assump_cell_style_right},
            {"headerName": "Bips", "editable": False, "valueFormatter": "x == null ? '' : Number(x).toFixed(2)", "cellStyle": assump_cell_style_default, "valueGetter": JsCode("""
                function(params) {
                    if (!params.node.group) { return params.data['GP Bips (Assump)']; }
                    const gp = params.node.aggData['GP ex gst (Assump)'];
                    const ttv = params.node.aggData['TTV (Assump)'];
                    if (ttv > 0) { return (gp / ttv) * 10000; }
                    return 0;
                }
            """)}
        ]}
    ],
    "defaultColDef": {"resizable": True, "width": 110},
    "groupDefaultExpanded": 0,
    "suppressAggFuncInHeader": True,
    "autoGroupColumnDef": {
        'headerName': 'Group', 'minWidth': 200, 'pinned': 'left',
        'cellRendererParams': {'suppressCount': True},
        'headerClass': 'purple-header'
    },
    "domLayout": "normal",
    "pinnedBottomRowData": total_row_data,
    "getRowStyle": JsCode("""
        function(params) {
            if (params.data && params.data.Merchant === '__ADJUSTMENT__') {
                return { 'display': 'none' };
            }
            if (params.node.isRowPinned()) {
                return { 'font-weight': 'bold', 'background-color': '#5D3A9B', 'color': 'white', 'font-size': '14px' };
            }
            // Style for Business Unit group row (level 0) or 'All' merchant group row
            if (params.node.group && (params.node.level === 0 || params.node.key === 'All')) {
                 return { 'font-weight': 'bold' };
            }
        }
    """),
    "rememberGroupStateWhenNewData": True,
    "onFirstDataRendered": restore_expanded_js,
}

grid_response = AgGrid(
    df_for_grid,
    gridOptions=grid_options,
    enable_enterprise_modules=True,
    update_mode='MODEL_CHANGED',
    theme='material',
    height=600,
    allow_unsafe_jscode=True,
    key=grid_key_with_counter,
    custom_js=get_expanded_js
)

# Store expanded keys in session state
if 'expanded_keys' not in st.session_state:
    st.session_state['expanded_keys'] = []
if 'custom' in grid_response and 'expanded' in grid_response['custom']:
    st.session_state['expanded_keys'] = grid_response['custom']['expanded']

# The data from the grid is the most up-to-date source
df_from_grid = pd.DataFrame(grid_response['data']) if grid_response['data'] is not None else processed_data.copy()

if update_button:
    # Recalculate and store the data against the base key.
    st.session_state.edited_data[grid_key_base] = recalculate_data(df_from_grid)
    # Increment the counter to force AgGrid to re-render from scratch on the next run.
    st.session_state.update_counter += 1
    st.rerun()

if surcharge_ban_button:
    # Use the data currently in the grid display for the calculation
    df_surcharge = df_for_grid.copy()
    debit_mask = df_surcharge['Card Type'].isin(['EFTPOS', 'Dom.DR', 'Prem.DR'])
    df_surcharge.loc[debit_mask, 'MSF Bips (Assump)'] = 65
    st.session_state.edited_data[grid_key_base] = recalculate_data(df_surcharge)
    st.session_state.update_counter += 1
    st.rerun()

# Custom CSS
st.markdown('''
    <style>
    /* More specific selector for action buttons */
    div[data-testid="stHorizontalBlock"] div[data-testid="stButton"] button {
        background-color: #5D3A9B !important; /* Purple from total row */
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        padding: 0.6em 1.2em !important;
        font-size: 15px !important;
        font-weight: 600 !important;
        transition: background-color 0.2s, color 0.2s !important;
        box-shadow: none !important;
    }
    div[data-testid="stHorizontalBlock"] div[data-testid="stButton"] button:hover {
        background-color: #492B7C !important; /* Darker Purple */
        color: white !important;
        border: none !important;
    }
    .purple-header {
        background-color: #5D3A9B !important;
    }
    .ag-theme-material .ag-cell {
        font-size: 12px !important;
    }
    .ag-theme-material .ag-header-cell, .ag-theme-material .ag-header-group-cell {
        background-color: #5D3A9B !important;
    }
    .ag-theme-material .ag-header-cell-text, .ag-theme-material .ag-header-group-text,
    .ag-theme-material .ag-header-cell-menu-button .ag-icon, .ag-theme-material .ag-sort-indicator-icon .ag-icon {
        color: white !important; font-weight: bold !important;
    }
    .center-aligned-header {
        background-color: #5D3A9B !important;
    }
    .center-aligned-header .ag-header-group-text {
        text-align: center !important; width: 100% !important;
    }
    </style>
''', unsafe_allow_html=True)