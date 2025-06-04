import streamlit as st
import pandas as pd
import snowflake.connector
from config import SNOWFLAKE_CONFIG

st.set_page_config(page_title="Oolio Group Feb-25  Adyen Advanced", layout="wide")
st.title("Model TTV")

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

# Card types and labels (same as assumptions.py)
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
payment_method_labels = {
    'AMEX': 'Amex',
    'EFTPOS': 'EFTPOS',
    'VC/MC Domestic Credit': 'VC/MC Domestic Credit',
    'VC/MC Domestic Debit': 'VC/MC Domestic Debit',
    'VC/MC Premium Credit': 'VC/MC Premium Credit',
    'VC/MC Premium Debit': 'VC/MC Premium Debit',
    'VC/MC Int.Credit': 'VC/MC Int.Credit',
    'VC/MC Int.Debit': 'VC/MC Int.Debit',
}

# Query for total TTV and TTV by card type
query_total = """
SELECT SUM(TTV) AS TOTAL_TTV FROM dia_db.public.rba_base_model
"""
query_by_type = """
SELECT
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
    SUM(TTV) AS TTV
FROM dia_db.public.rba_base_model
GROUP BY 1
ORDER BY 1
"""
df_total = run_query(query_total)
df_by_type = run_query(query_by_type)
total_ttv = df_total['TOTAL_TTV'].iloc[0] if not df_total.empty else 0
# Build lookup for base TTV by card type
base_ttv_lookup = {row['PAYMENT_METHOD']: row['TTV'] for _, row in df_by_type.iterrows()}

# Get assumptions from session state
def get_assumption(pm):
    if 'assumptions' in st.session_state and pm in st.session_state['assumptions']:
        val = st.session_state['assumptions'][pm]
        return float(val) if val is not None else 0.0
    return 0.0

# Build table data
rows = []
total_base_percent = 0.0
total_base_ttv = 0.0
total_new_percent = 0.0
total_new_ttv = 0.0
for pm in payment_method_order:
    label = payment_method_labels[pm]
    base_ttv = base_ttv_lookup.get(pm, 0)
    base_percent = (base_ttv / total_ttv * 100) if total_ttv else 0
    percent = get_assumption(pm)
    new_ttv = percent / 100 * total_ttv
    rows.append({
        'Card type': label,
        'base_percent': base_percent,
        'base_ttv': base_ttv,
        'New assumptions percent': percent,
        'New TTV': new_ttv
    })
    total_base_percent += base_percent
    total_base_ttv += base_ttv
    total_new_percent += percent
    total_new_ttv += new_ttv
# Add total row
rows.append({
    'Card type': 'Total',
    'base_percent': total_base_percent,
    'base_ttv': total_base_ttv,
    'New assumptions percent': total_new_percent,
    'New TTV': total_new_ttv
})
df_model = pd.DataFrame(rows)
df_model['base_percent'] = df_model['base_percent'].map(lambda x: f"{x:.2f}%")
df_model['base_ttv'] = df_model['base_ttv'].map(lambda x: f"${x:,.0f}")
df_model['New assumptions percent'] = df_model['New assumptions percent'].map(lambda x: f"{x:.2f}%")
df_model['New TTV'] = df_model['New TTV'].map(lambda x: f"${x:,.0f}")

def highlight_base_new_cols(s):
    base_cols = ['base_percent', 'base_ttv']
    new_cols = ['New assumptions percent', 'New TTV']
    color_base = 'background-color: #e3f2fd'
    color_new = 'background-color: #e8f5e9'
    return [
        color_base if col in base_cols else
        color_new if col in new_cols else
        '' for col in s.index
    ]

def bold_first_last_rows(row):
    if row.name == 0 or row.name == len(df_model) - 1:
        return ['font-weight: bold'] * len(row)
    return [''] * len(row)

def highlight_total_row(row):
    if row.name == len(df_model) - 1:
        return ['background-color: #ececec'] * len(row)
    return [''] * len(row)

styled_model = df_model.style.apply(highlight_base_new_cols, axis=1).apply(bold_first_last_rows, axis=1).apply(highlight_total_row, axis=1)
st.dataframe(styled_model, use_container_width=True, hide_index=True) 