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
    /* % of Total MSF column */
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
st.header("MSF by Card type")

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
    
    # Query to get data for pivot table with stricter filtering
    main_query = """
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
    
    # Get the data and create pivot table
    df = run_query(main_query)
    
    # Define the payment method order
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
    
    # Create pivot table without margins first
    pivot_table = pd.pivot_table(
        df,
        values='TOTAL_MSF',
        index=['Business Unit'],
        columns=['PAYMENT_METHOD'],
        aggfunc='sum',
        fill_value=0
    )
    
    # Remove the 'Other' column if it exists and keep it only as a row
    if 'Other' in pivot_table.columns:
        pivot_table = pivot_table.drop('Other', axis=1)
    
    # Reorder the columns according to payment_method_order
    pivot_table = pivot_table.reindex(columns=payment_method_order)
    
    # Calculate row totals and column totals
    row_totals = pivot_table.sum(axis=1)
    col_totals = pivot_table.sum()
    grand_total = col_totals.sum()
    
    # Add Total row
    pivot_table.loc['Total'] = col_totals
    
    # Format all values as currency using the newer style
    formatted_pivot = pivot_table.copy()
    formatted_pivot.index.name = "Business Unit"
    
    # Create DataFrame with both values and percentages
    rows = []
    
    # Define the desired order of business units
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
    
    # Process each business unit
    for index in business_unit_order:
        if index in pivot_table.index:
            row_values = pivot_table.loc[index]
            row_total = row_totals[index] if index != 'Total' else grand_total
            
            # First row - dollar values
            dollar_row = {}
            for column in payment_method_order:
                value = float(row_values[column])
                dollar_row[f"{column}\nMSF"] = format_currency(value)
            
            # Add row total
            dollar_row["Total\nMSF"] = format_currency(row_total)
            
            # Calculate % of total MSF
            if index != 'Total':
                pct_of_total = (row_total / grand_total * 100) if grand_total > 0 else 0
                dollar_row["% Of Total MSF\n%"] = f"{pct_of_total:.1f}%"
            else:
                dollar_row["% Of Total MSF\n%"] = "100.0%"
            
            # Second row - percentages of row total
            pct_row = {}
            for column in payment_method_order:
                value = float(row_values[column])
                if index != 'Total':
                    pct = (value / row_total * 100) if row_total > 0 else 0
                else:
                    pct = (value / grand_total * 100) if grand_total > 0 else 0
                pct_row[f"{column}\nMSF"] = f"{pct:.1f}%"
            pct_row["Total\nMSF"] = "100.0%"
            pct_row["% Of Total MSF\n%"] = ""
            
            # Add both rows with MultiIndex
            rows.append((index, "$ Amount", dollar_row))
            rows.append((index, "% of MSF", pct_row))
    
    # Create final table with MultiIndex
    multi_index = pd.MultiIndex.from_tuples([(x[0], x[1]) for x in rows], names=["Business Unit", "Type"])
    final_table = pd.DataFrame([x[2] for x in rows], index=multi_index)
    
    # Reset index to make it compatible with AgGrid
    final_table_display = final_table.reset_index()
    
    # Add a column for row type to control styling
    final_table_display['row_type'] = final_table_display['Type'].map(lambda x: 'bold' if x == '$ Amount' else 'normal')
    
    # Set the Business Unit column values
    final_table_display.loc[final_table_display['Type'] == '% of MSF', 'Business Unit'] = '% of MSF'
    
    # Configure grid options
    gb = GridOptionsBuilder.from_dataframe(final_table_display)
    gb.configure_default_column(
        resizable=True,
        sorteable=False,
        filterable=False,
        groupable=False
    )
    
    # Configure specific columns
    gb.configure_column("Business Unit", pinned="left", width=150)
    gb.configure_column("Type", hide=True)  # Hide the Type column
    gb.configure_column("row_type", hide=True)  # Hide the row type column
    for col in payment_method_order:
        gb.configure_column(f"{col}\nMSF", width=120)
    gb.configure_column("Total\nMSF", width=120)
    gb.configure_column("% Of Total MSF\n%", width=100)
    
    # Configure cell styling
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
    
    # Display the table using AgGrid
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

    # Show summary statistics
    st.header("Summary")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Business Units", len(df['Business Unit'].unique()))
    with col2:
        st.metric("Total Payment Methods", len(df['PAYMENT_METHOD'].unique()))
    with col3:
        st.metric("Total MSF", format_currency(df['TOTAL_MSF'].sum()))

except Exception as e:
    st.error(f"Error connecting to Snowflake: {str(e)}")
    st.info("Please make sure you have set up your .env file with the correct Snowflake credentials.") 