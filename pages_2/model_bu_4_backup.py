import streamlit as st
import pandas as pd
import snowflake.connector
from datetime import datetime, timedelta
from st_aggrid import AgGrid, GridOptionsBuilder, JsCode
from config import SNOWFLAKE_CONFIG
import numpy as np
from st_aggrid.shared import JsCode
import numbers
import difflib

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
# default_month = get_previous_month()  # Commented out - previously used last month
default_month = "2025-02"  # Set to specific month as requested
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
            MERCHANT_ACCOUNT,
            TRADING_MONTH,
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
        GROUP BY 1, 2, 3, 4, 5, 6, 7
    )
    SELECT 
        "Business Unit",
        "Merchant",
        MERCHANT_ACCOUNT,
        TRADING_MONTH,
        "Card Type",
        PAYMENT_METHOD,
        ACQUIRER,
        SUM(TTV) as TTV,
        SUM(MSF) as MSF,
        SUM(COA) as COA,
        SUM(SURCHARGE) as SURCHARGE
    FROM base_data
    GROUP BY 1, 2, 3, 4, 5, 6, 7
    ORDER BY "Business Unit", "Merchant", MERCHANT_ACCOUNT, TRADING_MONTH, "Card Type"
    """
    df = pd.read_sql(query, conn)
    
    # --- Vectorized Data Processing (Replaces slow loops) ---
    if df.empty:
        return pd.DataFrame()

    # --- After SQL, print columns for debug ---
    print('AFTER SQL:', df.columns.tolist())

    # 1. Separate data by acquirer type for easier processing
    adyen_data = df[df['ACQUIRER'].isin(['adyen_managed', 'adyen_balance'])].copy()
    wpay_data = df[df['ACQUIRER'].str.contains('wpay', case=False, na=False)].copy()

    # 2. Create the "All" merchant aggregate for Adyen data (no MERCHANT_ACCOUNT or TRADING_MONTH for 'All')
    adyen_agg = adyen_data.groupby(['Business Unit', 'Card Type']).agg(
        TTV=('TTV', 'sum'),
        MSF=('MSF', 'sum'),
        COA=('COA', 'sum'),
        SURCHARGE=('SURCHARGE', 'sum')
    ).reset_index()

    # 3. Process the complex WPay logic efficiently (no MERCHANT_ACCOUNT or TRADING_MONTH for 'All')
    wpay_agg_rows = []
    for bu in wpay_data['Business Unit'].unique():
        bu_wpay_data = wpay_data[wpay_data['Business Unit'] == bu]
        wpay_amex = bu_wpay_data[bu_wpay_data['PAYMENT_METHOD'] == 'AMEX'].sum(numeric_only=True)
        wpay_eftpos = bu_wpay_data[bu_wpay_data['PAYMENT_METHOD'] == 'EFTPOS'].sum(numeric_only=True)
        wpay_total = bu_wpay_data.sum(numeric_only=True)
        wpay_rest = wpay_total - wpay_amex - wpay_eftpos
        if wpay_amex['TTV'] > 0: wpay_agg_rows.append({'Business Unit': bu, 'Card Type': 'AMEX', **wpay_amex})
        if wpay_eftpos['TTV'] > 0: wpay_agg_rows.append({'Business Unit': bu, 'Card Type': 'EFTPOS', **wpay_eftpos})
    wpay_agg = pd.DataFrame(wpay_agg_rows)

    # 4. Combine Adyen and WPay aggregates to form the final "All" merchant data
    all_merchants_agg = pd.concat([adyen_agg, wpay_agg]).groupby(['Business Unit', 'Card Type']).sum().reset_index()
    all_merchants_agg['Merchant'] = 'All'
    all_merchants_agg['MERCHANT_ACCOUNT'] = None  # Add this column for consistency
    all_merchants_agg['TRADING_MONTH'] = None     # Add this column for consistency

    # 5. Get individual merchant data (ensure required columns exist)
    group_cols = ['Business Unit', 'Merchant', 'MERCHANT_ACCOUNT', 'TRADING_MONTH', 'Card Type']
    numeric_cols = ['TTV', 'MSF', 'COA', 'SURCHARGE']
    individual_merchants_grouped = (
        df.groupby(group_cols)[numeric_cols].sum().reset_index()
    )

    # 6. Combine "All" aggregate with grouped individual merchant data
    result_df = pd.concat([all_merchants_agg, individual_merchants_grouped], ignore_index=True)

    # 7. Final processing (GP, adjustments, sorting) - same as before
    result_df.fillna(0, inplace=True)
    result_df['GP'] = result_df['MSF'] - result_df['COA']

    # --- Efficiently Calculate '% of TTV' at multiple levels ---
    bu_total_ttv_map = result_df[result_df['Merchant'] == 'All'].groupby('Business Unit')['TTV'].sum()
    merchant_total_ttv_map = result_df[result_df['Merchant'] != 'All'].groupby(['Business Unit', 'Merchant', 'MERCHANT_ACCOUNT'])['TTV'].sum()

    result_df['bu_ttv_total'] = result_df['Business Unit'].map(bu_total_ttv_map)
    merchant_map = result_df.set_index(['Business Unit', 'Merchant', 'MERCHANT_ACCOUNT']).index.map(merchant_total_ttv_map)
    result_df['merchant_ttv_total'] = merchant_map

    result_df['% of TTV'] = np.nan
    mask_bu = (result_df['Merchant'] == 'All') & (result_df['bu_ttv_total'] > 0)
    result_df.loc[mask_bu, '% of TTV'] = (result_df['TTV'] / result_df['bu_ttv_total']) * 100
    mask_merchant = (result_df['Merchant'] != 'All') & (result_df['merchant_ttv_total'] > 0)
    result_df.loc[mask_merchant, '% of TTV'] = (result_df['TTV'] / result_df['merchant_ttv_total']) * 100
    result_df.drop(columns=['bu_ttv_total', 'merchant_ttv_total'], inplace=True)

    # Restore adjustment row logic: for each BU, add an Adjustment row as the negative sum of merchant rows
    final_rows = []
    metric_cols = ['TTV', 'MSF', 'COA', 'GP', 'SURCHARGE']
    for bu, group_df in result_df.groupby('Business Unit'):
        final_rows.extend(group_df.to_dict('records'))
        individual_merchants_df = group_df[(group_df['Merchant'] != 'All') & (group_df['MERCHANT_ACCOUNT'].notnull())]
        if not individual_merchants_df.empty:
            adjustment_values = individual_merchants_df[metric_cols].sum()
            adjustment_row = {'Business Unit': bu, 'Merchant': '__ADJUSTMENT__', 'MERCHANT_ACCOUNT': None}
            for col in metric_cols:
                adjustment_row[col] = -adjustment_values[col]
            final_rows.append(adjustment_row)

    result_df = pd.DataFrame(final_rows)

    if 'Card Type' in result_df.columns:
        result_df['sorter'] = np.where(result_df['Merchant'] == 'All', 0, 1)
        result_df['Card Type'] = pd.Categorical(result_df['Card Type'].fillna(''), categories=[''] + payment_method_order, ordered=True)
        result_df = result_df.sort_values(['Business Unit', 'sorter', 'Merchant', 'MERCHANT_ACCOUNT', 'Card Type']).drop('sorter', axis=1)

    # --- Before merge, print columns for debug ---
    print('BEFORE MERGE:', result_df.columns.tolist())

    return result_df

@st.cache_data
def process_data(df):
    """Applies initial processing and calculations to the dataframe."""
    if df.empty:
        return df

    # --- Data Processing and BIPS Calculation ---
    numeric_cols_to_fill = ['TTV', 'MSF', 'COA', 'GP', 'SURCHARGE']
    for col in numeric_cols_to_fill:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    # Remove ex GST calculations, use raw MSF, COA, GP values
    df['MSF ex gst'] = df['MSF']
    df['COA ex gst'] = df['COA']
    df['GP ex gst'] = df['MSF'] - df['COA']
    df['MSF Bips'] = np.where(df['TTV'] > 0, (df['MSF'] / df['TTV']) * 10000, 0)
    df['COA Bips'] = np.where(df['TTV'] > 0, (df['COA'] / df['TTV']) * 10000, 0)
    df['GP Bips'] = np.where(df['TTV'] > 0, (df['GP'] / df['TTV']) * 10000, 0)

    # --- New, Correct Percentage Calculation ---
    parent_ttv_map = df.groupby(['Business Unit', 'Merchant'])['TTV'].sum()
    df['parent_ttv'] = df.set_index(['Business Unit', 'Merchant']).index.map(parent_ttv_map)
    df['% of Parent Total'] = np.where(df['parent_ttv'] > 0, (df['TTV'] / df['parent_ttv']) * 100, 0)
    df.drop(columns=['parent_ttv'], inplace=True)

    # --- Initialize Assumption Columns ---
    df['TTV (Base)'] = df['TTV']
    df['TTV (Assump)'] = df['TTV']
    df['% of Parent Total (Assump)'] = df['% of Parent Total']
    df['MSF ex gst (Assump)'] = df['MSF ex gst']
    df['MSF Bips (Assump)'] = df['MSF Bips']
    df['COA ex gst (Assump)'] = df['COA ex gst']
    df['COA Bips (Assump)'] = df['COA Bips']
    df['GP ex gst (Assump)'] = df['GP ex gst']
    df['GP Bips (Assump)'] = df['GP Bips']
    
    # Convert numpy types to Python types for JSON serialization
    df = convert_numpy_types(df)
    
    return df

def convert_numpy_types(df):
    """
    Convert numpy types to Python types to ensure JSON serialization compatibility with AgGrid.
    """
    df_copy = df.copy()
    
    # First pass: convert column dtypes
    for col in df_copy.columns:
        if df_copy[col].dtype in ['int64', 'int32', 'float64', 'float32']:
            df_copy[col] = df_copy[col].astype(object)
    
    # Second pass: convert individual values
    def safe_convert(x):
        if pd.isna(x):
            return None
        elif isinstance(x, np.integer):
            return int(x)
        elif isinstance(x, np.floating):
            return float(x)
        elif isinstance(x, (int, float)):
            return x
        else:
            return x
    
    # Apply conversion to all columns
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].apply(safe_convert)
    
    # Final check: ensure no numpy types remain
    def final_check(obj):
        if isinstance(obj, (np.integer, np.floating, np.ndarray)):
            if isinstance(obj, np.ndarray):
                return obj.tolist()
            elif isinstance(obj, np.integer):
                return int(obj)
            elif isinstance(obj, np.floating):
                return float(obj)
        return obj
    
    # Apply final check to all values
    for col in df_copy.columns:
        df_copy[col] = df_copy[col].apply(final_check)
    
    return df_copy

def recalculate_data(df):
    """
    When % of Parent Total (Assump) is changed for a card type, only that row's TTV (Assump) is updated using the new % and the correct group TTV (Base):
    - If 'All' row: use BU total TTV (Base)
    - If merchant row: use merchant total TTV (Base)
    All other rows and columns remain unchanged.
    """
    df_copy = df.copy()

    # Ensure correct data types to prevent calculation errors
    numeric_cols = [
        'TTV', 'TTV (Assump)', 'TTV (Base)', '% of Parent Total (Assump)'
    ]
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)

    # Only update the edited row's TTV (Assump) based on new %
    mask = df_copy['Card Type'].isin(payment_method_order)
    diff = (df_copy['% of Parent Total (Assump)'] - df_copy['% of Parent Total']).abs()
    edited_rows = df_copy[mask & (diff > 1e-6)]
    for idx, row in edited_rows.iterrows():
        if row['Merchant'] == 'All':
            base_ttv = df_copy[(df_copy['Business Unit'] == row['Business Unit']) & (df_copy['Merchant'] == 'All')]['TTV (Base)'].sum()
        else:
            base_ttv = df_copy[(df_copy['Business Unit'] == row['Business Unit']) & (df_copy['Merchant'] == row['Merchant'])]['TTV (Base)'].sum()
        new_pct = row['% of Parent Total (Assump)']
        old_ttv_assump = row['TTV (Assump)']
        new_ttv_assump = new_pct / 100.0 * base_ttv
        df_copy.at[idx, 'TTV (Assump)'] = new_ttv_assump

    # Clean up
    for col in ['TTV', 'TTV (Assump)', 'TTV (Base)', '% of Parent Total (Assump)']:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna(0)
    
    # Convert numpy types to Python types for JSON serialization
    df_copy = convert_numpy_types(df_copy)
    
    return df_copy

def apply_surcharge_ban(df):
    """
    Applies surcharge ban by setting MSF Bips to 65 for debit cards.
    This function only updates MSF and GP calculations without affecting TTV values.
    """
    df_copy = df.copy()

    # Ensure correct data types to prevent calculation errors
    numeric_cols = [
        'TTV', 'TTV (Assump)', '% of Parent Total (Assump)',
        'MSF ex gst', 'MSF Bips', 'MSF ex gst (Assump)', 'MSF Bips (Assump)',
        'COA ex gst', 'COA Bips', 'COA ex gst (Assump)', 'COA Bips (Assump)',
        'GP ex gst', 'GP Bips', 'GP ex gst (Assump)', 'GP Bips (Assump)'
    ]
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)

    # Step 1: Identify debit card types and card type rows
    debit_mask = df_copy['Card Type'].isin(['EFTPOS', 'Dom.DR', 'Prem.DR'])
    card_type_mask = df_copy['Card Type'].isin(payment_method_order)

    # Step 2: Apply surcharge ban - set MSF Bips to 65 for debit cards
    df_copy.loc[debit_mask & card_type_mask, 'MSF Bips (Assump)'] = 65

    # Step 3: Recalculate MSF (Assump) from BASE TTV and updated Bips (only for affected rows)
    affected_mask = debit_mask & card_type_mask
    df_copy.loc[affected_mask, 'MSF ex gst (Assump)'] = \
        df_copy.loc[affected_mask, 'TTV'] * df_copy.loc[affected_mask, 'MSF Bips (Assump)'] / 10000

    # Step 4: Recalculate GP (Assump) for affected rows
    df_copy.loc[affected_mask, 'GP ex gst (Assump)'] = \
        df_copy.loc[affected_mask, 'MSF ex gst (Assump)'] - df_copy.loc[affected_mask, 'COA ex gst (Assump)']
    
    # Step 5: Recalculate GP Bips (Assump) for affected rows
    ttv_assump = df_copy.loc[affected_mask, 'TTV (Assump)']
    gp_assump = df_copy.loc[affected_mask, 'GP ex gst (Assump)']
    df_copy.loc[affected_mask, 'GP Bips (Assump)'] = \
        np.where(ttv_assump > 0, (gp_assump / ttv_assump) * 10000, 0)

    # Step 6: Clean up
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna(0)
    
    # Convert numpy types to Python types for JSON serialization
    df_copy = convert_numpy_types(df_copy)
    
    return df_copy

# --- New function for No Surcharge on Debit+ Increase Credit Surcharge ---
def apply_no_surcharge_increase_credit(df):
    df_copy = df.copy()
    numeric_cols = [
        'TTV', 'TTV (Assump)', '% of Parent Total (Assump)',
        'MSF ex gst', 'MSF Bips', 'MSF ex gst (Assump)', 'MSF Bips (Assump)',
        'COA ex gst', 'COA Bips', 'COA ex gst (Assump)', 'COA Bips (Assump)',
        'GP ex gst', 'GP Bips', 'GP ex gst (Assump)', 'GP Bips (Assump)'
    ]
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
    card_col = df_copy['Card Type'].astype(str).str.strip()
    # Step 1: Set Debit Bips to 65
    debit_mask = card_col.isin(['EFTPOS', 'Dom.DR', 'Prem.DR'])
    card_type_mask = card_col.isin(payment_method_order)
    df_copy.loc[debit_mask & card_type_mask, 'MSF Bips (Assump)'] = 65
    # Step 2: Set universal Bips for 5 card types regardless of acquirer
    universal_bips = {
        'AMEX': 170,
        'Dom.CR': 160,
        'Prem.CR': 160,
        'Int.CR': 250,
        'Int.DR': 250
    }
    for card, bips in universal_bips.items():
        mask = card_col == card
        df_copy.loc[mask, 'MSF Bips (Assump)'] = bips
    # Step 3: Recalculate MSF (Assump) from BASE TTV and updated Bips (only for affected rows)
    affected_mask = (
        debit_mask |
        card_col.isin(list(universal_bips.keys()))
    ) & card_type_mask
    df_copy.loc[affected_mask, 'MSF ex gst (Assump)'] = \
        df_copy.loc[affected_mask, 'TTV'] * df_copy.loc[affected_mask, 'MSF Bips (Assump)'] / 10000
    # Step 4: Recalculate GP (Assump) for affected rows
    df_copy.loc[affected_mask, 'GP ex gst (Assump)'] = \
        df_copy.loc[affected_mask, 'MSF ex gst (Assump)'] - df_copy.loc[affected_mask, 'COA ex gst (Assump)']
    # Step 5: Recalculate GP Bips (Assump) for affected rows
    ttv_assump = df_copy.loc[affected_mask, 'TTV (Assump)']
    gp_assump = df_copy.loc[affected_mask, 'GP ex gst (Assump)']
    df_copy.loc[affected_mask, 'GP Bips (Assump)'] = \
        np.where(ttv_assump > 0, (gp_assump / ttv_assump) * 10000, 0)
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna(0)
    
    # Convert numpy types to Python types for JSON serialization
    df_copy = convert_numpy_types(df_copy)
    
    return df_copy

# --- New function for Reduce COA on Credit Card ---
def apply_reduce_coa_credit(df):
    df_copy = df.copy()
    numeric_cols = [
        'TTV', 'TTV (Assump)', '% of Parent Total (Assump)',
        'MSF ex gst', 'MSF Bips', 'MSF ex gst (Assump)', 'MSF Bips (Assump)',
        'COA ex gst', 'COA Bips', 'COA ex gst (Assump)', 'COA Bips (Assump)',
        'GP ex gst', 'GP Bips', 'GP ex gst (Assump)', 'GP Bips (Assump)'
    ]
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)
    card_col = df_copy['Card Type'].astype(str).str.strip()
    # Reduce COA Bips (Assump) for credit cards
    # Dom.CR: -10, Prem.CR/Int.CR/Int.DR: -30
    domcr_mask = card_col == 'Dom.CR'
    premcr_mask = card_col == 'Prem.CR'
    intcr_mask = card_col == 'Int.CR'
    intdr_mask = card_col == 'Int.DR'
    df_copy.loc[domcr_mask, 'COA Bips (Assump)'] = df_copy.loc[domcr_mask, 'COA Bips (Assump)'] - 10
    df_copy.loc[premcr_mask | intcr_mask | intdr_mask, 'COA Bips (Assump)'] = df_copy.loc[premcr_mask | intcr_mask | intdr_mask, 'COA Bips (Assump)'] - 30
    # Also set MSF Bips (Assump) for these card types as in Increase Credit Surcharge button
    universal_bips = {
        'AMEX': 170,
        'Dom.CR': 160,
        'Prem.CR': 160,
        'Int.CR': 250,
        'Int.DR': 250
    }
    for card, bips in universal_bips.items():
        mask = card_col == card
        df_copy.loc[mask, 'MSF Bips (Assump)'] = bips
    # Recalculate MSF (Assump), COA (Assump), and dependent values for affected rows
    affected_mask = (
        domcr_mask | premcr_mask | intcr_mask | intdr_mask | card_col.isin(list(universal_bips.keys()))
    )
    df_copy.loc[affected_mask, 'MSF ex gst (Assump)'] = df_copy.loc[affected_mask, 'TTV'] * df_copy.loc[affected_mask, 'MSF Bips (Assump)'] / 10000
    df_copy.loc[affected_mask, 'COA ex gst (Assump)'] = df_copy.loc[affected_mask, 'TTV (Assump)'] * df_copy.loc[affected_mask, 'COA Bips (Assump)'] / 10000
    df_copy.loc[affected_mask, 'GP ex gst (Assump)'] = df_copy.loc[affected_mask, 'MSF ex gst (Assump)'] - df_copy.loc[affected_mask, 'COA ex gst (Assump)']
    ttv_assump = df_copy.loc[affected_mask, 'TTV (Assump)']
    gp_assump = df_copy.loc[affected_mask, 'GP ex gst (Assump)']
    df_copy.loc[affected_mask, 'GP Bips (Assump)'] = np.where(ttv_assump > 0, (gp_assump / ttv_assump) * 10000, 0)
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna(0)
    
    # Convert numpy types to Python types for JSON serialization
    df_copy = convert_numpy_types(df_copy)
    
    return df_copy

# --- New function for Scenario 4: Card Mix Changes ---
def apply_card_mix_scenario(df):
    """
    Optimized: Applies card mix scenario by changing TTV assumption percentages and recalculating all assumption values.
    Uses custom card type percentages for Scenario 4, applied per BU or merchant group, vectorized for speed.
    """
    df_copy = df.copy()
    
    # Ensure correct data types
    numeric_cols = [
        'TTV', 'TTV (Assump)', '% of Parent Total (Assump)',
        'MSF ex gst', 'MSF Bips', 'MSF ex gst (Assump)', 'MSF Bips (Assump)',
        'COA ex gst', 'COA Bips', 'COA ex gst (Assump)', 'COA Bips (Assump)',
        'GP ex gst', 'GP Bips', 'GP ex gst (Assump)', 'GP Bips (Assump)'
    ]
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce').fillna(0)

    # Card type percentages for Scenario 4
    card_type_percentages = {
        'AMEX': 0.05,      # 5%
        'EFTPOS': 0.40,    # 40%
        'Dom.DR': 0.25,    # 25%
        'Dom.CR': 0.04,    # 4%
        'Prem.DR': 0.07,   # 7%
        'Prem.CR': 0.15,   # 15%
        'Int.DR': 0.02,    # 2%
        'Int.CR': 0.02     # 2%
    }
    
    # Only apply to rows that are card types in payment_method_order
    card_type_mask = df_copy['Card Type'].isin(payment_method_order)
    df_types = df_copy[card_type_mask].copy()
    
    # Determine group columns
    group_cols = []
    if 'Business Unit' in df_copy.columns:
        group_cols.append('Business Unit')
    if 'Merchant' in df_copy.columns:
        group_cols.append('Merchant')
    
    # Compute group total TTV for each row
    df_types['Group_TTV'] = df_types.groupby(group_cols)['TTV'].transform('sum')
    # Map card type percentages
    df_types['CardTypePct'] = df_types['Card Type'].map(card_type_percentages).fillna(0)
    # Calculate new TTV (Assump) and %
    df_types['TTV (Assump)'] = df_types['CardTypePct'] * df_types['Group_TTV']
    df_types['% of Parent Total (Assump)'] = df_types['CardTypePct'] * 100
    # MSF/COA/GP assumption values
    df_types['MSF ex gst (Assump)'] = df_types['TTV (Assump)'] * df_types['MSF Bips (Assump)'] / 10000
    df_types['COA ex gst (Assump)'] = df_types['TTV (Assump)'] * df_types['COA Bips (Assump)'] / 10000
    df_types['GP ex gst (Assump)'] = df_types['MSF ex gst (Assump)'] - df_types['COA ex gst (Assump)']
    df_types['GP Bips (Assump)'] = np.where(
        df_types['TTV (Assump)'] > 0,
        (df_types['GP ex gst (Assump)'] / df_types['TTV (Assump)']) * 10000,
        0
    )
    # Update the main DataFrame only for card type rows
    for col in ['TTV (Assump)', '% of Parent Total (Assump)', 'MSF ex gst (Assump)', 'COA ex gst (Assump)', 'GP ex gst (Assump)', 'GP Bips (Assump)']:
        df_copy.loc[df_types.index, col] = df_types[col]
    # Clean up
    for col in numeric_cols:
        if col in df_copy.columns:
            df_copy[col] = df_copy[col].fillna(0)
    df_copy = convert_numpy_types(df_copy)
    return df_copy

# --- New function for Scenario 5: Churn (15% churn after Scenario 4) ---
def apply_churn_scenario(df):
    """
    Applies Scenario 4 (card mix), then multiplies all Assump values for each Business Unit and 'All' merchant by 0.85 (15% churn), and recalculates Assump % and Bips.
    """
    # Step 1: Apply Scenario 4 (card mix)
    df_churn = apply_card_mix_scenario(df.copy())
    # Step 2: For each Business Unit and 'All' merchant, multiply all Assump values by 0.85
    churn_factor = 0.85
    # Only apply to rows that are card types in payment_method_order
    card_type_mask = df_churn['Card Type'].isin(payment_method_order)
    # For each group (Business Unit, Merchant), apply churn to 'All' merchant rows and group rows
    group_cols = []
    if 'Business Unit' in df_churn.columns:
        group_cols.append('Business Unit')
    if 'Merchant' in df_churn.columns:
        group_cols.append('Merchant')
    # Apply churn to all card type rows
    churn_mask = card_type_mask
    # Multiply all Assump values by churn_factor
    for col in ['TTV (Assump)', 'MSF ex gst (Assump)', 'COA ex gst (Assump)', 'GP ex gst (Assump)']:
        if col in df_churn.columns:
            df_churn.loc[churn_mask, col] = df_churn.loc[churn_mask, col] * churn_factor
    # Recalculate Assump % and Bips
    # For each group, recalculate group total TTV (Assump)
    df_churn['Group_TTV_Assump'] = df_churn.groupby(group_cols)['TTV (Assump)'].transform('sum')
    df_churn['% of Parent Total (Assump)'] = np.where(
        df_churn['Group_TTV_Assump'] > 0,
        df_churn['TTV (Assump)'] / df_churn['Group_TTV_Assump'] * 100,
        0
    )
    # Recalculate Bips
    df_churn['MSF Bips (Assump)'] = np.where(
        df_churn['TTV (Assump)'] > 0,
        df_churn['MSF ex gst (Assump)'] / df_churn['TTV (Assump)'] * 10000,
        0
    )
    df_churn['COA Bips (Assump)'] = np.where(
        df_churn['TTV (Assump)'] > 0,
        df_churn['COA ex gst (Assump)'] / df_churn['TTV (Assump)'] * 10000,
        0
    )
    df_churn['GP Bips (Assump)'] = np.where(
        df_churn['TTV (Assump)'] > 0,
        df_churn['GP ex gst (Assump)'] / df_churn['TTV (Assump)'] * 10000,
        0
    )
    # Clean up
    for col in ['Group_TTV_Assump']:
        if col in df_churn.columns:
            df_churn.drop(columns=[col], inplace=True)
    df_churn = convert_numpy_types(df_churn)
    return df_churn

# --- Main data loading and caching ---
raw_data = get_metric_data(bu_filter, merchant_filter, acquirer_filter, month_filter, account_manager_filter)
processed_data = process_data(raw_data)

# After processed_data is defined, store the true base data in session state if not already present
if 'base_processed_data' not in st.session_state:
    st.session_state['base_processed_data'] = processed_data.copy()

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

# Filter out adjustment rows before displaying or exporting
if 'Merchant' in df_for_grid.columns:
    # df_for_grid = df_for_grid[df_for_grid['Merchant'] != '__ADJUSTMENT__']
    pass

st.markdown("<h5>Interactive Buttons</h5>", unsafe_allow_html=True)

# --- Buttons above the table ---
col1, col2, col3, col4, col5, col6 = st.columns([1, 1, 1, 1, 1, 2])
with col1:
    update_button = st.button("Update Model Table", type="primary", help="Apply assumption changes to the model table")
with col2:
    surcharge_ban_button = st.button("Scenario 1", type="primary", help="Set MSF Bips for Debit cards to 65")
with col3:
    no_surcharge_credit_button = st.button("Scenario 2", type="primary", help="Set Debit Bips to 65 and increase Credit Surcharge Bips as specified")
with col4:
    reduce_coa_credit_button = st.button("Scenario 3", type="primary", help="Reduce COA Bips on credit cards as per business rule")
with col5:
    card_mix_button = st.button("Scenario 4", type="primary", help="Apply custom card mix percentages (AMEX:5%, EFTPOS:40%, Dom.DR:25%, etc.) and recalculate assumption values")
with col6:
    churn_button = st.button("Scenario 5", type="primary", help="Apply Scenario 4 with 15% churn (multiply all Assump values by 85%)")

# --- Scenario 5 button handler ---
if churn_button:
    st.session_state.edited_data[grid_key_base] = apply_churn_scenario(df_for_grid.copy())
    st.session_state.update_counter += 1
    st.rerun()

# --- Display Grid ---
total_row_data = None
grand_total_ttv = 0
grand_total_ttv_assump = 0
if not df_for_grid.empty:
    all_rows_df = df_for_grid[(df_for_grid['Merchant'] == 'All') & (df_for_grid['Card Type'].isin(payment_method_order))]
    if not all_rows_df.empty:
        grand_total_ttv = all_rows_df['TTV'].sum()
        grand_total_ttv_assump = all_rows_df['TTV (Assump)'].sum()
    total_row = {}
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

# Final check: ensure df_for_grid has no numpy types before rendering AgGrid
df_for_grid = convert_numpy_types(df_for_grid)

# Always reset index and applymap to ensure all values are Python types
def force_python_types(val):
    if pd.isna(val):
        return None
    if isinstance(val, (np.integer, int)):
        return int(val)
    if isinstance(val, (np.floating, float)):
        return float(val)
    return val

# Reset index and convert all values
# (this replaces the previous conversion block before AgGrid)
df_for_grid = df_for_grid.reset_index(drop=True)
df_for_grid = df_for_grid.astype(object).applymap(force_python_types)

# Also ensure columns are str (sometimes pandas can have numpy types as column names)
df_for_grid.columns = [str(col) for col in df_for_grid.columns]

# If the DataFrame is empty, show an empty table
if df_for_grid.empty:
    st.info("No data available for the selected merchant(s). Showing empty table.")
    AgGrid(pd.DataFrame(columns=df_for_grid.columns))
    st.stop()

# If only one row, try to force a second dummy row (diagnostic)
if len(df_for_grid) == 1:
    dummy = {col: None for col in df_for_grid.columns}
    df_for_grid = pd.concat([df_for_grid, pd.DataFrame([dummy])], ignore_index=True)

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

# --- JS Value Getters and Setters for Percentage Columns ---
base_pct_getter = JsCode(f"""
function(params) {{
    const grandTotalTTV = {grand_total_ttv or 0};
    if (params.node && params.node.group) {{
        const groupTTV = params.node.aggData && params.node.aggData.TTV ? params.node.aggData.TTV : 0;
        return grandTotalTTV > 0 ? (groupTTV / grandTotalTTV) * 100 : 0;
    }}
    return params.data ? params.data['% of Parent Total'] : null;
}}
""")

assump_pct_getter = JsCode(f"""
function(params) {{
    const grandTotalTTV = {grand_total_ttv_assump or 0};
    if (params.node && params.node.group) {{
        const groupTTV = params.node.aggData && params.node.aggData['TTV (Assump)'] ? params.node.aggData['TTV (Assump)'] : 0;
        return grandTotalTTV > 0 ? (groupTTV / grandTotalTTV) * 100 : 0;
    }}
    return params.data ? params.data['% of Parent Total (Assump)'] : null;
}}
""")

assump_pct_setter = JsCode("""
function(params) {
    if (params.data && !params.node.group) {
        // Update the underlying data field when a user edits the cell
        params.data['% of Parent Total (Assump)'] = params.newValue;
        return true;
    }
    return false;
}
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
            # Use the new JS valueGetter for the Base % column
            {"headerName": "%", "valueGetter": base_pct_getter, "valueFormatter": "value == null ? '' : Number(value).toFixed(2) + '%'"},
            {"field": "TTV (Assump)", "headerName": "Assump", "aggFunc": "sum", "editable": True, "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": assump_cell_style_right},
            # Use the new JS valueGetter and valueSetter for the Assump % column
            {"headerName": "%", "editable": True, "valueGetter": assump_pct_getter, "valueSetter": assump_pct_setter, "valueFormatter": "value == null ? '' : Number(value).toFixed(2) + '%'", "cellStyle": assump_cell_style_default}
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
            {
                "headerName": "Bips",
                "editable": True,
                "valueFormatter": "x == null ? '' : Number(x).toFixed(2)",
                "cellStyle": assump_cell_style_default,
                "valueGetter": JsCode("""
                    function(params) {
                        if (!params.node.group) { return params.data['COA Bips (Assump)']; }
                        const coa = params.node.aggData['COA ex gst (Assump)'];
                        const ttv = params.node.aggData['TTV (Assump)'];
                        if (ttv > 0) { return (coa / ttv) * 10000; }
                        return 0;
                    }
                """),
                "valueSetter": JsCode("""
                    function(params) {
                        if (params.data && !params.node.group) {
                            params.data['COA Bips (Assump)'] = Number(params.newValue);
                            return true;
                        }
                        return false;
                    }
                """)
            }
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
        ]},
        {"headerName": "Incentives", "headerClass": "center-aligned-header", "children": [
            {"field": "INCENTIVES_BASE", "headerName": "Base", "type": "numericColumn", "valueGetter": JsCode("""
                function(params) {{
                    if (!params.data || params.data['Card Type']) return '';
                    return params.data['INCENTIVES_BASE'];
                }}
            """), "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": {"textAlign": "right"}},
            {"field": "INCENTIVES_BIPS", "headerName": "Bips", "type": "numericColumn", "valueGetter": JsCode("""
                function(params) {{
                    if (!params.data || params.data['Card Type']) return '';
                    return params.data['INCENTIVES_BIPS'];
                }}
            """), "valueFormatter": "x == null ? '' : Number(x).toFixed(2)", "cellStyle": {"textAlign": "right"}},
            {"field": "INCENTIVES_ASSUMP_BASE", "headerName": "Assump", "type": "numericColumn", "valueGetter": JsCode("""
                function(params) {{
                    if (!params.data || params.data['Card Type']) return '';
                    return params.data['INCENTIVES_ASSUMP_BASE'];
                }}
            """), "valueFormatter": "x == null ? '' : x.toLocaleString(undefined, {maximumFractionDigits:0})", "cellStyle": {"textAlign": "right"}},
            {"field": "INCENTIVES_ASSUMP_BIPS", "headerName": "Bips", "type": "numericColumn", "valueGetter": JsCode("""
                function(params) {{
                    if (!params.data || params.data['Card Type']) return '';
                    return params.data['INCENTIVES_ASSUMP_BIPS'];
                }}
            """), "valueFormatter": "x == null ? '' : Number(x).toFixed(2)", "cellStyle": {"textAlign": "right"}}
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

# Store expanded keys in session state
if 'expanded_keys' not in st.session_state:
    st.session_state['expanded_keys'] = []

# The data from the grid is the most up-to-date source
df_from_grid = processed_data.copy()

if update_button:
    # Recalculate and store the data against the base key.
    st.session_state.edited_data[grid_key_base] = recalculate_data(df_from_grid)
    st.session_state.update_counter += 1
    st.rerun()

if surcharge_ban_button:
    st.session_state.edited_data[grid_key_base] = apply_surcharge_ban(df_for_grid.copy())
    st.session_state.update_counter += 1
    st.rerun()

if no_surcharge_credit_button:
    st.session_state.edited_data[grid_key_base] = apply_no_surcharge_increase_credit(df_for_grid.copy())
    st.session_state.update_counter += 1
    st.rerun()

if reduce_coa_credit_button:
    st.session_state.edited_data[grid_key_base] = apply_reduce_coa_credit(df_for_grid.copy())
    st.session_state.update_counter += 1
    st.rerun()

if card_mix_button:
    st.session_state.edited_data[grid_key_base] = apply_card_mix_scenario(df_for_grid.copy())
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

# --- Custom styled Download CSV button at top right ---
st.markdown("""
    <style>
    .stDownloadButton[data-testid="stDownloadButton"][data-key="download-csv-btn"] > button {
        background-color: #fff;
        color: #111;
        border: 2px solid #111;
        border-radius: 6px;
        padding: 0.5em 1.5em;
        font-weight: 600;
        font-size: 1em;
        box-shadow: none;
        transition: border 0.2s;
    }
    .stDownloadButton[data-testid="stDownloadButton"][data-key="download-csv-btn"] > button:hover {
        border: 2px solid #333;
        color: #000;
    }
    </style>
""", unsafe_allow_html=True)

top_cols = st.columns([8, 1])
with top_cols[1]:
    csv = df_for_grid.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Download as CSV",
        data=csv,
        file_name="model_bu_2_table.csv",
        mime="text/csv",
        key="download-csv-btn",
        help="Download the current model table (All and Merchant level, with card types) as CSV."
    )

gb = GridOptionsBuilder.from_dataframe(df_for_grid)
gb.configure_column("INCENTIVES", type=["numericColumn"], aggFunc="sum", valueFormatter="x.toLocaleString('en-US', {minimumFractionDigits: 2, maximumFractionDigits: 2})")
# ... configure other columns as needed ...
grid_options = gb.build()

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

# Now that grid_response is defined, we can access it
if 'custom' in grid_response and 'expanded' in grid_response['custom']:
    st.session_state['expanded_keys'] = grid_response['custom']['expanded']

# The data from the grid is the most up-to-date source
df_from_grid = pd.DataFrame(grid_response['data']) if grid_response['data'] is not None else processed_data.copy()

# --- Scenario Table with Default and 10 Scenarios ---

# Helper: get the current total row as a dict (or None)
def get_current_total_row():
    if total_row_data and len(total_row_data) > 0:
        return total_row_data[0].copy()
    return None

# Always initialize a scenario table in session state: first row is Default, next 10 are Scenario 1-10
scenario_labels = ['Default'] + [f'Scenario {i+1}' for i in range(10)]
key_cols = [
    'Description',
    'TTV', '% of TTV', 'TTV (Assump)', '% of TTV (Assump)',
    'MSF ex gst', 'MSF Bips', 'MSF ex gst (Assump)', 'MSF Bips (Assump)',
    'COA ex gst', 'COA Bips', 'COA ex gst (Assump)', 'COA Bips (Assump)',
    'GP ex gst', 'GP Bips', 'GP ex gst (Assump)', 'GP Bips (Assump)'
]

# Helper: format a row for display
currency_cols = ['TTV', 'TTV (Assump)', 'MSF ex gst', 'MSF ex gst (Assump)',
                 'COA ex gst', 'COA ex gst (Assump)', 'GP ex gst', 'GP ex gst (Assump)']
percent_cols = ['% of TTV', '% of TTV (Assump)']
bips_cols = ['MSF Bips', 'MSF Bips (Assump)', 'COA Bips', 'COA Bips (Assump)', 'GP Bips', 'GP Bips (Assump)']
def format_row(row, label):
    formatted = {col: '' for col in key_cols}
    for col in currency_cols:
        if col in row:
            try:
                formatted[col] = f"${row[col]:,.0f}" if pd.notnull(row[col]) else ''
            except Exception:
                formatted[col] = row[col]
    for col in percent_cols:
        if col in row:
            try:
                formatted[col] = f"{row[col]:.2f}%" if pd.notnull(row[col]) else ''
            except Exception:
                formatted[col] = row[col]
    for col in bips_cols:
        if col in row:
            try:
                formatted[col] = f"{row[col]:.2f}" if pd.notnull(row[col]) else ''
            except Exception:
                formatted[col] = row[col]
    formatted['Description'] = label
    return formatted

# Store the original default row in session state ONLY ONCE (on first load or after reset)
if 'scenario_default_row' not in st.session_state:
    base_row = get_current_total_row()
    if base_row:
        st.session_state['scenario_default_row'] = format_row(base_row, 'Default')
    else:
        st.session_state['scenario_default_row'] = {col: '' for col in key_cols}
        st.session_state['scenario_default_row']['Description'] = 'Default'

# Always initialize the scenario table rows if not present or wrong length
if 'scenario_table_rows' not in st.session_state or len(st.session_state['scenario_table_rows']) != len(scenario_labels):
    st.session_state['scenario_table_rows'] = []
    st.session_state['scenario_table_rows'].append(st.session_state['scenario_default_row'].copy())
    for label in scenario_labels[1:]:
        row = {col: '' for col in key_cols}
        row['Description'] = label
        st.session_state['scenario_table_rows'].append(row)
else:
    # Always keep Default row as the original (never update after first set)
    st.session_state['scenario_table_rows'][0] = st.session_state['scenario_default_row'].copy()

# --- Two-Step Scenario Management ---
# Step 1: Scenario buttons update main table (existing logic)
# Step 2: Confirm Scenario button saves current totals to selected scenario

# Initialize scenario selection in session state
if 'selected_scenario_to_save' not in st.session_state:
    st.session_state['selected_scenario_to_save'] = 1  # Default to Scenario 1

# Scenario selection and confirm button
st.markdown('---')
st.markdown('<h4 style="color:#5D3A9B;">Save Current Totals to Scenario</h4>', unsafe_allow_html=True)
col1, col2, col3 = st.columns([2, 2, 2])

with col1:
    scenario_to_save = st.selectbox(
        "Select Scenario to Save To:",
        options=[(i, f"Scenario {i}") for i in range(1, 11)],
        format_func=lambda x: x[1],
        index=st.session_state['selected_scenario_to_save'] - 1,
        key="scenario_selector"
    )
    st.session_state['selected_scenario_to_save'] = scenario_to_save[0]

with col2:
    confirm_scenario_button = st.button(
        f"Save to Scenario {scenario_to_save[0]}",
        help=f"Save the current totals to Scenario {scenario_to_save[0]}",
        key="confirm_scenario_btn"
    )

with col3:
    # Show current scenario status
    current_scenario_data = st.session_state['scenario_table_rows'][scenario_to_save[0]]
    has_data = any(str(current_scenario_data.get(col, '')).strip() for col in currency_cols + percent_cols + bips_cols if col != 'Description')
    status_text = "✅ Has Data" if has_data else "❌ Empty"
    st.markdown(f"**Status:** {status_text}")

# Handle confirm scenario button
if confirm_scenario_button:
    current_total = get_current_total_row()
    if current_total:
        st.session_state['scenario_table_rows'][scenario_to_save[0]] = format_row(current_total, f"Scenario {scenario_to_save[0]}")
        st.success(f"✅ Saved current totals to Scenario {scenario_to_save[0]}")
        st.rerun()

# Reset scenario table
if st.button('Reset Scenario Table'):
    base_row = get_current_total_row()
    if base_row:
        st.session_state['scenario_default_row'] = format_row(base_row, 'Default')
    else:
        st.session_state['scenario_default_row'] = {col: '' for col in key_cols}
        st.session_state['scenario_default_row']['Description'] = 'Default'
    st.session_state['scenario_table_rows'] = []
    st.session_state['scenario_table_rows'].append(st.session_state['scenario_default_row'].copy())
    for label in scenario_labels[1:]:
        row = {col: '' for col in key_cols}
        row['Description'] = label
        st.session_state['scenario_table_rows'].append(row)

# --- Display Scenario Table ---
st.markdown('---')
st.markdown('<h4 style="color:#5D3A9B;">Scenario Table</h4>', unsafe_allow_html=True)

# Add instructions for the scenario table
st.markdown('''
<div style="background-color:#f8f9fa; border-radius:8px; padding:15px; margin-bottom:15px;">
<h5 style="margin-top:0; color:#5D3A9B;">How to Use Scenarios:</h5>
<ol style="font-size:14px; margin-bottom:0;">
  <li><b>Apply a scenario</b> using the buttons above (Surcharge Ban, No Surcharge Increase Credit, etc.)</li>
  <li><b>Review the changes</b> in the main table above</li>
  <li><b>Select a scenario slot</b> (1-10) from the dropdown</li>
  <li><b>Click "Save to Scenario X"</b> to store the current totals</li>
  <li><b>Repeat</b> for different scenarios to compare results</li>
</ol>
</div>
''', unsafe_allow_html=True)

scenario_df = pd.DataFrame(st.session_state['scenario_table_rows'])
scenario_df = scenario_df[[col for col in key_cols if col in scenario_df.columns]]

# Highlight the currently selected scenario
def highlight_selected_scenario(row):
    if row['Description'] == f"Scenario {st.session_state['selected_scenario_to_save']}":
        return ['background-color: #fff3cd; font-weight: bold'] * len(row)
    return [''] * len(row)

styled = scenario_df.style
styled = styled.set_properties(**{'text-align': 'center'})
styled = styled.set_table_styles([
    {'selector': 'th', 'props': [('background-color', '#002b45'), ('color', 'white'), ('font-weight', 'bold'), ('text-align', 'center')]},
    {'selector': 'td', 'props': [('font-size', '15px')]},
])
styled = styled.apply(lambda x: ['background-color: #f2f9ff' if i%2==0 else 'background-color: #e6f0fa' for i in range(len(x))], axis=1)
styled = styled.apply(highlight_selected_scenario, axis=1)
st.table(styled)

# --- Fetch Incentives from rba_incentives and merge at merchant level ---
conn = init_snowflake_connection()
incentives_query = '''
SELECT
    "Business Unit" AS BUSINESS_UNIT,
    MERCHANT_ACCOUNT,
    "Merchant" AS MERCHANT,
    TRADING_MONTH,
    INCENTIVES
FROM dia_db.public.rba_incentives
'''
incentives_df = pd.read_sql(incentives_query, conn)
incentives_df.columns = [col.upper() for col in incentives_df.columns]

# Verify the incentives data (silent)
# print("\n=== INCENTIVES DATA VERIFICATION ===")
# print(f"Incentives table shape: {incentives_df.shape}")
# print(f"Unique Business Units in incentives: {incentives_df['BUSINESS_UNIT'].nunique()}")
# print(f"Unique months in incentives: {incentives_df['TRADING_MONTH'].nunique()}")

# Show incentive values by Business Unit (silent)
# print("\nIncentive values by Business Unit:")
# bu_incentives = incentives_df.groupby('BUSINESS_UNIT')['INCENTIVES'].agg(['count', 'sum', 'mean', 'min', 'max'])
# print(bu_incentives)

# Show sample of actual incentives data (silent)
# print("\nSample incentives data:")
# print(incentives_df[['BUSINESS_UNIT', 'MERCHANT', 'TRADING_MONTH', 'INCENTIVES']].head(20))

# SIMPLE APPROACH: Let's see what data we actually have (silent)
# print("\n=== DATA ANALYSIS ===")
# print(f"Main data shape: {processed_data.shape}")
# print(f"Incentives data shape: {incentives_df.shape}")

# Check what business units we have in each dataset (silent)
# print("\n=== BUSINESS UNITS COMPARISON ===")
main_bus = processed_data['Business Unit'].unique()
incentives_bus = incentives_df['BUSINESS_UNIT'].unique()
# print(f"Main data Business Units ({len(main_bus)}): {sorted([str(x) for x in main_bus])}")
# print(f"Incentives Business Units ({len(incentives_bus)}): {sorted([str(x) for x in incentives_bus])}")

# Check what months we have (silent)
# print("\n=== MONTHS COMPARISON ===")
main_months = processed_data['TRADING_MONTH'].unique()
incentives_months = incentives_df['TRADING_MONTH'].unique()
# print(f"Main data Months ({len(main_months)}): {sorted([str(x) for x in main_months])}")
# print(f"Incentives Months ({len(incentives_months)}): {sorted([str(x) for x in incentives_months])}")

# Check merchant-level data specifically (silent)
merchant_level_df = processed_data[processed_data['Card Type'].isnull() | (processed_data['Card Type'] == '')]
# print(f"\nMerchant-level rows in main data: {len(merchant_level_df)}")

# Show sample of merchant names from both datasets (silent)
# print("\n=== SAMPLE MERCHANT NAMES ===")
# print("MAIN DATA MERCHANTS (first 20):")
# for i, merchant in enumerate(merchant_level_df['Merchant'].unique()[:20]):
#     print(f"  {i+1}. {merchant}")

# print("\nINCENTIVES MERCHANTS (first 20):")
# for i, merchant in enumerate(incentives_df['MERCHANT'].unique()[:20]):
#     print(f"  {i+1}. {merchant}")

# Try exact matching first (silent)
# print("\n=== EXACT MATCHING TEST ===")
exact_matches = 0
for idx, row in merchant_level_df.iterrows():
    bu = str(row['Business Unit'])
    merchant = str(row['Merchant'])
    month = str(row['TRADING_MONTH'])
    
    # Look for exact match
    match = incentives_df[
        (incentives_df['BUSINESS_UNIT'].astype(str) == bu) & 
        (incentives_df['MERCHANT'].astype(str) == merchant) & 
        (incentives_df['TRADING_MONTH'].astype(str) == month)
    ]
    
    if len(match) > 0:
        exact_matches += 1
        # print(f"EXACT MATCH: {merchant} (BU: {bu}, Month: {month}) -> Incentives: {match.iloc[0]['INCENTIVES']}")

# print(f"\nExact matches found: {exact_matches} out of {len(merchant_level_df)}")

# Try case-insensitive matching (silent)
# print("\n=== CASE-INSENSITIVE MATCHING TEST ===")
case_insensitive_matches = 0
for idx, row in merchant_level_df.iterrows():
    bu = str(row['Business Unit']).upper()
    merchant = str(row['Merchant']).upper()
    month = str(row['TRADING_MONTH'])
    
    # Look for case-insensitive match
    match = incentives_df[
        (incentives_df['BUSINESS_UNIT'].astype(str).str.upper() == bu) & 
        (incentives_df['MERCHANT'].astype(str).str.upper() == merchant) & 
        (incentives_df['TRADING_MONTH'].astype(str) == month)
    ]
    
    if len(match) > 0:
        case_insensitive_matches += 1
        # print(f"CASE-INSENSITIVE MATCH: {row['Merchant']} (BU: {row['Business Unit']}, Month: {month}) -> Incentives: {match.iloc[0]['INCENTIVES']}")

# print(f"\nCase-insensitive matches found: {case_insensitive_matches} out of {len(merchant_level_df)}")

# Try matching by Business Unit and Month only (ignore merchant name) (silent)
# print("\n=== BUSINESS UNIT + MONTH MATCHING TEST ===")
bu_month_matches = 0
for idx, row in merchant_level_df.iterrows():
    bu = str(row['Business Unit']).upper()
    month = str(row['TRADING_MONTH'])
    
    # Look for matches by BU and month only
    matches = incentives_df[
        (incentives_df['BUSINESS_UNIT'].astype(str).str.upper() == bu) & 
        (incentives_df['TRADING_MONTH'].astype(str) == month)
    ]
    
    if len(matches) > 0:
        bu_month_matches += 1
        # print(f"BU+MONTH MATCH: {row['Merchant']} (BU: {row['Business Unit']}, Month: {month})")
        # print(f"  Available incentives merchants: {list(matches['MERCHANT'].unique())}")

# print(f"\nBU+Month matches found: {bu_month_matches} out of {len(merchant_level_df)}")

# Remove old diagnostic code that references non-existent columns

# --- Assign Incentives using Business Unit and Month matching ---
# Since MERCHANT_ID and MERCHANT_ACCOUNT formats don't match, use Business Unit + Month approach
processed_data['TRADING_MONTH'] = processed_data['TRADING_MONTH'].astype(str)
incentives_df['TRADING_MONTH'] = incentives_df['TRADING_MONTH'].astype(str)

# Create a lookup dictionary: (Business Unit, Month) -> list of (Merchant, Incentive) pairs
incentives_lookup = {}
for _, row in incentives_df.iterrows():
    bu = str(row['BUSINESS_UNIT']).upper()
    month = str(row['TRADING_MONTH'])
    merchant = str(row['MERCHANT']).upper()
    incentive = row['INCENTIVES']
    
    key = (bu, month)
    if key not in incentives_lookup:
        incentives_lookup[key] = []
    incentives_lookup[key].append((merchant, incentive))

# Function to find best merchant match
def find_best_merchant_match(target_merchant, available_merchants):
    """
    Find the best matching merchant from available options using fuzzy matching
    """
    target_merchant = str(target_merchant).upper()
    
    # First try exact match
    for merchant, incentive in available_merchants:
        if str(merchant).upper() == target_merchant:
            return incentive
    
    # Then try partial match
    for merchant, incentive in available_merchants:
        if target_merchant in str(merchant).upper() or str(merchant).upper() in target_merchant:
            return incentive
    
    # Finally, try fuzzy matching
    best_match = None
    best_ratio = 0
    for merchant, incentive in available_merchants:
        ratio = difflib.SequenceMatcher(None, target_merchant, str(merchant).upper()).ratio()
        if ratio > best_ratio and ratio > 0.6:  # Only consider matches with >60% similarity
            best_ratio = ratio
            best_match = incentive
    
    return best_match

# Apply incentives to merchant-level rows
merchant_level_mask = processed_data['Card Type'].isnull() | (processed_data['Card Type'] == '')
incentives_assigned = 0

for idx, row in processed_data[merchant_level_mask].iterrows():
    bu = str(row['Business Unit']).upper()
    month = str(row['TRADING_MONTH'])
    merchant = str(row['Merchant']).upper()
    
    key = (bu, month)
    
    if key in incentives_lookup:
        # Try to find a matching merchant
        incentive = find_best_merchant_match(merchant, incentives_lookup[key])
        if incentive is not None:
            processed_data.at[idx, 'INCENTIVES'] = incentive
            incentives_assigned += 1
        else:
            # If no merchant match, use the first available incentive for this BU+Month
            processed_data.at[idx, 'INCENTIVES'] = incentives_lookup[key][0][1]
            incentives_assigned += 1
    else:
        processed_data.at[idx, 'INCENTIVES'] = 0.0

# Fill NaN incentives with 0.0 for any remaining rows
processed_data['INCENTIVES'] = processed_data['INCENTIVES'].fillna(0.0)

# Diagnostic: Show how many incentives were assigned
print(f"\n=== INCENTIVES ASSIGNMENT SUMMARY ===")
print(f"Total incentives assigned: {incentives_assigned}")
print(f"Total merchant-level rows: {len(processed_data[merchant_level_mask])}")

# Show sample of assigned incentives
print(f"\n=== SAMPLE ASSIGNED INCENTIVES ===")
sample_assigned = processed_data[merchant_level_mask][['Business Unit', 'Merchant', 'TRADING_MONTH', 'INCENTIVES']].head(10)
for _, row in sample_assigned.iterrows():
    print(f"  {row['Merchant']} (BU: {row['Business Unit']}, Month: {row['TRADING_MONTH']}) -> {row['INCENTIVES']}")

# Show non-zero incentives
non_zero_count = len(processed_data[merchant_level_mask][processed_data[merchant_level_mask]['INCENTIVES'] != 0])
print(f"\nMerchants with non-zero incentives: {non_zero_count}")

# --- Calculate Incentives columns ---
is_merchant_level = processed_data['Card Type'].isnull() | (processed_data['Card Type'] == '')
processed_data.loc[is_merchant_level, 'INCENTIVES_BASE'] = processed_data.loc[is_merchant_level, 'INCENTIVES']
processed_data.loc[is_merchant_level, 'INCENTIVES_BIPS'] = (
    processed_data.loc[is_merchant_level, 'INCENTIVES'] / processed_data.loc[is_merchant_level, 'TTV']
).replace([np.inf, -np.inf], 0).fillna(0) * 10000
processed_data.loc[is_merchant_level, 'INCENTIVES_ASSUMP_BASE'] = processed_data.loc[is_merchant_level, 'INCENTIVES_BASE']
processed_data.loc[is_merchant_level, 'INCENTIVES_ASSUMP_BIPS'] = processed_data.loc[is_merchant_level, 'INCENTIVES_BIPS']

is_card_type = ~is_merchant_level
for col in ['INCENTIVES', 'INCENTIVES_BASE', 'INCENTIVES_BIPS', 'INCENTIVES_ASSUMP_BASE', 'INCENTIVES_ASSUMP_BIPS']:
    processed_data.loc[is_card_type, col] = np.nan

df_for_grid = processed_data[processed_data['Card Type'].isnull() | (processed_data['Card Type'] == '')].copy()

print("\n=== MAIN DATA: BUSINESS UNIT & TRADING_MONTH SAMPLE ===")
print(processed_data[['Business Unit', 'TRADING_MONTH']].drop_duplicates().head(20))

print("\n=== INCENTIVES DATA: BUSINESS_UNIT & TRADING_MONTH SAMPLE ===")
print(incentives_df[['BUSINESS_UNIT', 'TRADING_MONTH']].drop_duplicates().head(20))

print("=== DATAFRAME USED IN TABLE ===")
print(df_for_grid[['Business Unit', 'Merchant', 'TRADING_MONTH', 'INCENTIVES']].head(20))
print("Non-zero incentives in table data:")
print(df_for_grid[df_for_grid['INCENTIVES'] != 0][['Business Unit', 'Merchant', 'TRADING_MONTH', 'INCENTIVES']].head(20))

df_for_grid['INCENTIVES'] = 123.45
print(df_for_grid[['INCENTIVES']].head(20))