import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
from config import SNOWFLAKE_CONFIG

st.set_page_config(page_title="Oolio Group Feb-25  Adyen Advanced", layout="wide")

st.title("Oolio Group Feb-25 Adyen Advanced")
st.header("GP Summary")

@st.cache_resource
def init_snowflake_connection():
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

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

def format_currency(value):
    if pd.isna(value) or value == 0:
        return "$0"
    return f"${round(value):,}"

try:
    conn = init_snowflake_connection()
    # Query for all metrics by business unit and payment method
    summary_query = """
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
        SUM(TTV) AS GTV,
        SUM(ABS(TOTAL_MSF)) AS MSF,
        SUM(ABS(TOTAL_ACQUIRER_FEE)) AS COGS
    FROM dia_db.public.rba_base_model
    GROUP BY 1, 2
    ORDER BY 1, 2
    """
    df = run_query(summary_query)
    df["GP"] = df["MSF"] - df["COGS"]
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
        'Other'
    ]
    metric_order = ["GTV", "MSF", "COGS", "GP"]
    metric_labels = {"GTV": "GTV", "MSF": "MSF", "COGS": "COGS", "GP": "GP"}
    rows = []
    metric_totals = {metric: {pm: 0 for pm in payment_method_order} for metric in metric_order}
    metric_totals_sum = {metric: 0 for metric in metric_order}
    for bu in business_unit_order:
        bu_df = df[df["Business Unit"] == bu]
        if bu_df.empty:
            continue
        for i, metric in enumerate(metric_order):
            row = {"Business Unit": bu if i == 0 else "", "Metric": metric_labels[metric]}
            total = 0
            for pm in payment_method_order:
                val = bu_df[bu_df["PAYMENT_METHOD"] == pm][metric].sum()
                row[pm] = format_currency(val)
                total += val
                metric_totals[metric][pm] += val
            row["Total"] = format_currency(total)
            metric_totals_sum[metric] += total
            row["row_type"] = "bold"
            rows.append(row)
    # Calculate grand totals for percent columns
    grand_totals = {metric: metric_totals_sum[metric] for metric in metric_order}
    # Add % of Total column
    for row in rows:
        metric = row["Metric"]
        total_val = float(row["Total"].replace("$", "").replace(",", ""))
        if metric == "GTV":
            percent = (total_val / grand_totals[metric]) * 100 if grand_totals[metric] != 0 else 0
            row["% of Total"] = f"{percent:.2f}%"
        else:
            row["% of Total"] = ""
    # Add total rows at the bottom for each metric
    for i, metric in enumerate(metric_order):
        total_row = {"Business Unit": "Total" if i == 0 else "", "Metric": metric_labels[metric]}
        total_sum = 0
        for pm in payment_method_order:
            val = metric_totals[metric][pm]
            total_row[pm] = format_currency(val)
            total_sum += val
        total_row["Total"] = format_currency(total_sum)
        total_row["% of Total"] = "100.00%" if metric == "GTV" else ""
        total_row["row_type"] = "bold"
        rows.append(total_row)
    summary_table = pd.DataFrame(rows)
    display_columns = ["Business Unit", "Metric"] + payment_method_order + ["Total", "% of Total", "row_type"]
    summary_table = summary_table[display_columns]
    # Configure AgGrid options for formatting
    from st_aggrid import AgGrid, GridOptionsBuilder
    gb = GridOptionsBuilder.from_dataframe(summary_table)
    gb.configure_default_column(resizable=True, sorteable=False, filterable=False, groupable=False)
    gb.configure_column("Business Unit", pinned="left", width=150)
    gb.configure_column("Metric", pinned="left", width=80)
    gb.configure_column("row_type", hide=True)
    for col in payment_method_order:
        gb.configure_column(col, width=120)
    gb.configure_column("Total", width=120)
    cell_style = {
        "styleConditions": [
            {
                "condition": "params.data.row_type == 'bold'",
                "style": {"font-weight": "bold"}
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
        summary_table,
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
except Exception as e:
    st.error(f"Error connecting to Snowflake: {str(e)}")
    st.info("Please make sure you have set up your .env file with the correct Snowflake credentials.") 