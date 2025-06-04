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
# Remove the dashboard intro/title/description at the top of the TTV page

# Page title at the very top
st.subheader("TTV Summary by Card Type")

# Map internal acquirer values to user-friendly display names
acquirer_display_map = {
    'adyen_managed': 'Adyen Managed',
    'adyen_balance': 'Adyen Balance',
    'wpay': 'Wpay'
}
acquirer_display_reverse = {v: k for k, v in acquirer_display_map.items()}

# Get filter options
business_units, acquirers, months = get_filter_options()
default_month = get_previous_month()

# --- Combined Table Filters ---
months_desc = sorted(months, reverse=True)

# Add 'All' option to each filter
all_bu = ["All"] + business_units
all_acquirer = ["All"] + [acquirer_display_map.get(a, a) for a in acquirers]
all_months = ["All"] + months_desc

col1, col2, col3 = st.columns(3)
with col1:
    selected_bu = st.multiselect("Business Unit", all_bu, default=["All"])
with col2:
    selected_acquirer = st.multiselect("Acquirer", all_acquirer, default=["All"])
with col3:
    selected_months = st.multiselect("Trading Month", all_months, default=[default_month])

# Map selected display names back to internal values for filtering
selected_acquirer_internal = [acquirer_display_reverse.get(a, a) for a in selected_acquirer]

# Handle 'All' logic for each filter
def get_selected_or_all(selected, all_values):
    if "All" in selected or not selected:
        return all_values
    return selected

bu_filter = get_selected_or_all(selected_bu, business_units)
acquirer_filter = get_selected_or_all(selected_acquirer_internal, acquirers)
month_filter = get_selected_or_all(selected_months, months_desc)

# --- Combined Table Data ---
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
                WHEN SOURCE = 'Swiftpos_Reseller' THEN 'SwiftPOS Reseller'
                WHEN SOURCE = 'OolioPay' THEN 'Oolio Pay'
                WHEN SOURCE = 'IdealPOS_Reseller' THEN 'IdealPOS Reseller'
                WHEN SOURCE IN ('Oolio', 'OolioPaymentPlatform') THEN 'Oolio Platform'
                ELSE SOURCE
            END AS "Business Unit",
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
            LOWER(ACQUIRER) as ACQUIRER,
            SUM(ABS(TTV)) as VALUE
        FROM dia_db.public.rba_model_data
        WHERE {where_clause}
        GROUP BY 1, 2, 3
    )
    SELECT 
        "Business Unit",
        "Card Type",
        ACQUIRER,
        SUM(VALUE) as VALUE
    FROM base_data
    GROUP BY 1, 2, 3
    ORDER BY "Business Unit", "Card Type"
    """
    df = pd.read_sql(query, conn)
    return df

# Get data
data = get_metric_data(bu_filter, acquirer_filter, month_filter)

# Build table rows
rows = []
grand_total = 0

for bu in data['Business Unit'].unique():
    bu_data = data[data['Business Unit'] == bu]
    row = {'Business Unit': bu}
    total = 0
    card_type_values = {}

    # Calculate values for each card type
    for card_type in payment_method_order:
        # Only Wpay selected
        if acquirer_filter == ["Wpay"]:
            pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
            value = bu_data[bu_data['ACQUIRER'].str.contains('wpay', case=False)]['VALUE'].sum() * pct
        # Only Adyen Managed selected
        elif acquirer_filter == ["Adyen Managed"]:
            value = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['VALUE'].sum()
        # Only Adyen Balance selected
        elif acquirer_filter == ["Adyen Balance"]:
            value = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['VALUE'].sum()
        # Multiple acquirers (including All)
        else:
            # Adyen Managed actuals
            adyen_managed = bu_data[(bu_data['ACQUIRER'] == 'adyen_managed') & (bu_data['Card Type'] == card_type)]['VALUE'].sum()
            # Adyen Balance actuals
            adyen_balance = bu_data[(bu_data['ACQUIRER'] == 'adyen_balance') & (bu_data['Card Type'] == card_type)]['VALUE'].sum()
            # Wpay hardcoded split
            wpay_total = bu_data[bu_data['ACQUIRER'].str.contains('wpay', case=False)]['VALUE'].sum()
            wpay_pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
            wpay_value = wpay_total * wpay_pct
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
    if acquirer_filter == ["Wpay"]:
        pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
        value = data[data['ACQUIRER'].str.contains('wpay', case=False)]['VALUE'].sum() * pct
    elif acquirer_filter == ["Adyen Managed"]:
        value = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['VALUE'].sum()
    elif acquirer_filter == ["Adyen Balance"]:
        value = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['VALUE'].sum()
    else:
        adyen_managed = data[(data['ACQUIRER'] == 'adyen_managed') & (data['Card Type'] == card_type)]['VALUE'].sum()
        adyen_balance = data[(data['ACQUIRER'] == 'adyen_balance') & (data['Card Type'] == card_type)]['VALUE'].sum()
        wpay_total = data[data['ACQUIRER'].str.contains('wpay', case=False)]['VALUE'].sum()
        wpay_pct = next((pct for ct, pct in wpay_card_types if ct == card_type), 0)
        wpay_value = wpay_total * wpay_pct
        value = adyen_managed + adyen_balance + wpay_value
    total_row[card_type] = value
    total_sum += value
total_row['Total'] = total_sum
rows.append(total_row)

# Create DataFrame with both values and percentages
final_rows = []
for row in rows[:-1]:  # Exclude the total row for now
    # Add dollar amount row
    dollar_row = {'Business Unit': row['Business Unit']}
    for card_type in payment_method_order:
        dollar_row[card_type] = format_currency(row[card_type])
    dollar_row['Total'] = format_currency(row['Total'])
    dollar_row['% Of Total'] = f"{(row['Total'] / grand_total * 100):.2f}%"
    final_rows.append(dollar_row)
    
    # Add percentage row (weighted average, always sums to 100%)
    pct_row = {'Business Unit': '% of TTV'}
    for card_type in payment_method_order:
        pct = (row[card_type] / row['Total'] * 100) if row['Total'] > 0 else 0
        pct_row[card_type] = f"{pct:.2f}%"
    pct_row['Total'] = "100.00%"
    pct_row['% Of Total'] = ""
    final_rows.append(pct_row)

# Add Total row
total_dollar_row = {'Business Unit': 'Total'}
for card_type in payment_method_order:
    total_dollar_row[card_type] = format_currency(total_row[card_type])
total_dollar_row['Total'] = format_currency(total_sum)
total_dollar_row['% Of Total'] = "100.00%"
final_rows.append(total_dollar_row)

# Add Total percentage row
total_pct_row = {'Business Unit': '% of Total'}
for card_type in payment_method_order:
    pct = (total_row[card_type] / total_sum * 100) if total_sum > 0 else 0
    total_pct_row[card_type] = f"{pct:.2f}%"
total_pct_row['Total'] = "100.00%"
total_pct_row['% Of Total'] = ""
final_rows.append(total_pct_row)

# Create final DataFrame
final_df = pd.DataFrame(final_rows)

# Configure grid options
gb = GridOptionsBuilder.from_dataframe(final_df)
gb.configure_default_column(
    resizable=True,
    sorteable=False,
    filterable=False,
    groupable=False
)

# Configure specific columns
gb.configure_column("Business Unit", pinned="left", width=150)
for col in payment_method_order:
    gb.configure_column(col, width=120)
gb.configure_column("Total", width=120)
gb.configure_column("% Of Total", width=100)

# Configure cell styling
style_jscode = JsCode('''
function(params) {
    let row = params.node.rowIndex;
    let col = params.colDef.field;
    let totalRows = params.api.getDisplayedRowCount();
    let totalCols = params.columnApi && params.columnApi.getAllDisplayedColumns
        ? params.columnApi.getAllDisplayedColumns().length
        : 0;
    let colIndex = params.columnApi && params.columnApi.getAllDisplayedColumns
        ? params.columnApi.getAllDisplayedColumns().findIndex(c => c.colId === col)
        : -1;
    if ((row >= totalRows - 2) || (colIndex >= totalCols - 2 && colIndex !== -1)) {
        return { 'backgroundColor': '#f5e9da' };
    }
    return {};
}
''')

gb.configure_grid_options(
    suppressRowTransform=True,
    domLayout='normal',
    cellStyle=style_jscode
)

grid_options = gb.build()

# Display the table
AgGrid(
    final_df,
    gridOptions=grid_options,
    allow_unsafe_jscode=True,
    theme="material",
    height=500,
    fit_columns_on_grid_load=False,
    use_container_width=True,
    custom_css={
        ".ag-header-cell-label": {"justify-content": "center"},
        ".ag-cell": {"min-width": "120px"}
    }
)

# After displaying the main table, add the summary section at the bottom
st.markdown("---")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Business Units", len(data['Business Unit'].unique()))
with col2:
    st.metric("Total Payment Methods", len(payment_method_order))
with col3:
    st.metric("Total TTV", format_currency(data['VALUE'].sum())) 