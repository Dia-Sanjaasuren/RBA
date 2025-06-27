import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
from config import SNOWFLAKE_CONFIG
from st_aggrid import AgGrid, GridOptionsBuilder
from st_aggrid.grid_options_builder import GridOptionsBuilder

# Set page config
st.set_page_config(page_title="Oolio Group Feb-25  Adyen Advanced", layout="wide")

# Custom CSS for column widths
st.markdown("""
<style>
    /* First column (Business Unit) */
    .stTable td:nth-child(1) {
        min-width: 150px !important;
        max-width: 150px !important;
    }
    /* Payment type columns */
    .stTable td:nth-child(2),
    .stTable td:nth-child(3),
    .stTable td:nth-child(4),
    .stTable td:nth-child(5),
    .stTable td:nth-child(6),
    .stTable td:nth-child(7),
    .stTable td:nth-child(8),
    .stTable td:nth-child(9) {
        min-width: 120px !important;
        max-width: 120px !important;
    }
    /* Total column */
    .stTable td:nth-child(10) {
        min-width: 120px !important;
        max-width: 120px !important;
    }
    /* % of Total GP column */
    .stTable td:nth-child(11) {
        min-width: 100px !important;
        max-width: 100px !important;
    }
    /* Ensure text wrapping works properly */
    .stTable td {
        white-space: normal !important;
        word-wrap: break-word !important;
    }
    /* Make dollar amount rows bold */
    .ag-row .ag-cell[col-id="Business Unit"] {
        font-weight: normal;
    }
    .bold-row .ag-cell {
        font-weight: bold !important;
    }
    .normal-row .ag-cell {
        font-weight: normal !important;
    }
</style>
""", unsafe_allow_html=True)

# Title
st.title("Oolio Group Feb-25 Adyen Advanced")
st.header("GP by Card type")

# Establish Snowflake connection
@st.cache_resource
def init_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

# Function to run query and return pandas DataFrame
@st.cache_data
def run_query(query):
    conn = init_snowflake_connection()
    cur = conn.cursor()
    try:
        cur.execute(query)
        results = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        df = pd.DataFrame(results, columns=columns)
        return df
    finally:
        cur.close()

# Function to format currency values
def format_currency(value):
    if pd.isna(value) or value == 0:
        return "$0"
    return f"${round(value):,}"

# Main content
try:
    conn = init_snowflake_connection()
    
    # Query to get MSF data
    msf_query = """
    WITH base_data AS (
        SELECT 
            CASE 
                WHEN SOURCE = 'Swiftpos_Reseller' THEN 'SwiftPOS Reseller'
                WHEN SOURCE = 'OolioPay' THEN 'Oolio Pay'
                WHEN SOURCE = 'IdealPOS_Reseller' THEN 'IdealPOS Reseller'
                WHEN SOURCE = 'Oolio' THEN 'Oolio Platform'
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
            END AS PAYMENT_METHOD,
            SUM(ABS(TOTAL_MSF)) as TOTAL_MSF
        FROM dia_db.public.rba_base_model
        WHERE LOWER(ACQUIRER) = 'adyen_balance'
        AND TRADING_MONTH = '2025.02'
        GROUP BY 
            CASE 
                WHEN SOURCE = 'Swiftpos_Reseller' THEN 'SwiftPOS Reseller'
                WHEN SOURCE = 'OolioPay' THEN 'Oolio Pay'
                WHEN SOURCE = 'IdealPOS_Reseller' THEN 'IdealPOS Reseller'
                WHEN SOURCE = 'Oolio' THEN 'Oolio Platform'
                ELSE SOURCE
            END,
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
            END
    )
    SELECT *
    FROM base_data
    ORDER BY "Business Unit", PAYMENT_METHOD
    """
    
    # Query to get COA data
    coa_query = """
    WITH base_data AS (
        SELECT 
            CASE 
                WHEN SOURCE = 'Swiftpos_Reseller' THEN 'SwiftPOS Reseller'
                WHEN SOURCE = 'OolioPay' THEN 'Oolio Pay'
                WHEN SOURCE = 'IdealPOS_Reseller' THEN 'IdealPOS Reseller'
                WHEN SOURCE = 'Oolio' THEN 'Oolio Platform'
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
            END AS PAYMENT_METHOD,
            SUM(ABS(TOTAL_ACQUIRER_FEE)) as TOTAL_ACQUIRER_FEE
        FROM dia_db.public.rba_base_model
        WHERE LOWER(ACQUIRER) = 'adyen_balance'
        AND TRADING_MONTH = '2025.02'
        GROUP BY 
            CASE 
                WHEN SOURCE = 'Swiftpos_Reseller' THEN 'SwiftPOS Reseller'
                WHEN SOURCE = 'OolioPay' THEN 'Oolio Pay'
                WHEN SOURCE = 'IdealPOS_Reseller' THEN 'IdealPOS Reseller'
                WHEN SOURCE = 'Oolio' THEN 'Oolio Platform'
                ELSE SOURCE
            END,
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
            END
    )
    SELECT *
    FROM base_data
    ORDER BY "Business Unit", PAYMENT_METHOD
    """
    
    # Get the data and create pivot tables
    msf_df = run_query(msf_query)
    coa_df = run_query(coa_query)
    
    payment_method_order = [
        'AMEX',
        'EFTPOS',
        'VC/MC Domestic Credit',
        'VC/MC Domestic Debit',
        'VC/MC Premium Credit',
        'VC/MC Premium Debit',
        'VC/MC Int.Credit',
        'VC/MC Int.Debit'
    ]
    
    business_unit_order = [
        'Bepoz',
        'Deliverit',
        'Ordermate',
        'SwiftPOS',
        'SwiftPOS Reseller',
        'IdealPOS',
        'IdealPOS Reseller',
        'Oolio Platform',
        'Oolio Pay',
        'Other',
        'Total'
    ]
    
    msf_pivot = pd.pivot_table(
        msf_df,
        values='TOTAL_MSF',
        index=['Business Unit'],
        columns=['PAYMENT_METHOD'],
        aggfunc='sum',
        fill_value=0
    )
    coa_pivot = pd.pivot_table(
        coa_df,
        values='TOTAL_ACQUIRER_FEE',
        index=['Business Unit'],
        columns=['PAYMENT_METHOD'],
        aggfunc='sum',
        fill_value=0
    )
    
    # Align indexes and columns
    msf_pivot = msf_pivot.reindex(index=business_unit_order, columns=payment_method_order, fill_value=0)
    coa_pivot = coa_pivot.reindex(index=business_unit_order, columns=payment_method_order, fill_value=0)
    
    # Calculate GP as MSF - COA (both already Ex GST)
    gp_pivot = msf_pivot - coa_pivot
    
    # Calculate row totals and column totals
    row_totals = gp_pivot.sum(axis=1)
    col_totals = gp_pivot.sum()
    grand_total = col_totals.sum()
    gp_pivot.loc['Total'] = col_totals
    
    # Format all values as currency
    formatted_pivot = gp_pivot.copy()
    formatted_pivot.index.name = "Business Unit"
    
    rows = []
    for index in business_unit_order:
        if index in gp_pivot.index:
            row_values = gp_pivot.loc[index]
            msf_row_values = msf_pivot.loc[index] if index in msf_pivot.index else pd.Series([0]*len(payment_method_order), index=payment_method_order)
            row_total = row_totals[index] if index != 'Total' else grand_total
            # First row - dollar values
            dollar_row = {}
            for column in payment_method_order:
                value = float(row_values[column])
                dollar_row[f"{column}\nGP"] = format_currency(value)
            dollar_row["Total\nGP"] = format_currency(row_total)
            if index != 'Total':
                pct_of_total = (row_total / grand_total * 100) if grand_total > 0 else 0
                dollar_row["% Of Total GP\n%"] = f"{pct_of_total:.1f}%"
            else:
                dollar_row["% Of Total GP\n%"] = "100.0%"
            # Second row - GP(%) as (GP / MSF * 100)
            pct_row = {}
            for column in payment_method_order:
                msf_val = float(msf_row_values[column])
                gp_val = float(row_values[column])
                pct = (gp_val / msf_val * 100) if msf_val != 0 else 0
                pct_row[f"{column}\nGP"] = f"{pct:.1f}%"
            pct_row["Total\nGP"] = ""
            pct_row["% Of Total GP\n%"] = ""
            # Third row - % of Total GP (GP value / row_total * 100)
            totalgp_row = {}
            for column in payment_method_order:
                gp_val = float(row_values[column])
                totalgp = row_total
                pct = (gp_val / totalgp * 100) if totalgp != 0 else 0
                totalgp_row[f"{column}\nGP"] = f"{pct:.1f}%"
            totalgp_row["Total\nGP"] = "100.0%"
            totalgp_row["% Of Total GP\n%"] = ""
            rows.append((index, "$ Amount", dollar_row))
            rows.append((index, "GP(%)", pct_row))
            rows.append((index, "% of Total GP", totalgp_row))
    multi_index = pd.MultiIndex.from_tuples([(x[0], x[1]) for x in rows], names=["Business Unit", "Type"])
    final_table = pd.DataFrame([x[2] for x in rows], index=multi_index)
    final_table_display = final_table.reset_index()
    final_table_display['row_type'] = final_table_display['Type'].map(lambda x: 'bold' if x == '$ Amount' else 'normal')
    final_table_display.loc[final_table_display['Type'] == 'GP(%)', 'Business Unit'] = 'GP(%)'
    final_table_display.loc[final_table_display['Type'] == '% of Total GP', 'Business Unit'] = '% of Total GP'
    gb = GridOptionsBuilder.from_dataframe(final_table_display)
    gb.configure_default_column(
        resizable=True,
        sorteable=False,
        filterable=False,
        groupable=False
    )
    gb.configure_column("Business Unit", pinned="left", width=150)
    gb.configure_column("Type", hide=True)
    gb.configure_column("row_type", hide=True)
    for col in payment_method_order:
        gb.configure_column(f"{col}\nGP", width=120)
    gb.configure_column("Total\nGP", width=120)
    gb.configure_column("% Of Total GP\n%", width=100)
    cell_style = {
        "styleConditions": [
            {
                "condition": "params.data.row_type == 'bold'",
                "style": {"font-weight": "bold"}
            },
            {
                "condition": "params.data.row_type == 'normal'",
                "style": {"font-weight": "normal"}
            }
        ]
    }
    gb.configure_grid_options(
        suppressRowTransform=True,
        domLayout='normal',
        cellStyle=cell_style
    )
    grid_options = gb.build()
    AgGrid(
        final_table_display,
        gridOptions=grid_options,
        allow_unsafe_jscode=True,
        theme="material",
        height=900,
        fit_columns_on_grid_load=True,
        custom_css={
            ".ag-row-even": {"background-color": "#f5f5f5"},
            ".ag-row-odd": {"background-color": "#ffffff"}
        }
    )
    
    # Summary section below the table
    st.markdown("---")
    st.header("Summary")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Business Units", len(gp_pivot.index) - 1)
    with col2:
        st.metric("Total Payment Methods", len(payment_method_order))
    with col3:
        st.metric("Total GP (Ex GST)", format_currency(grand_total))
except Exception as e:
    st.error(f"Error connecting to Snowflake: {str(e)}")
    st.info("Please make sure you have set up your .env file with the correct Snowflake credentials.") 