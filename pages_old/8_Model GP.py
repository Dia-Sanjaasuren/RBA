import streamlit as st
import pandas as pd
import snowflake.connector
from config import SNOWFLAKE_CONFIG

st.set_page_config(page_title="Oolio Group Feb-25  Adyen Advanced", layout="wide")
st.title("Model GP")

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

# Payment method order and labels
payment_method_order = [
    'AMEX',
    'EFTPOS',
    'VC/MC Dom.CR',
    'VC/MC Dom.DR',
    'VC/MC Prem.CR',
    'VC/MC Prem.DR',
    'VC/MC Int.CR',
    'VC/MC Int.DR'
]
payment_method_labels = {
    'AMEX': 'Amex',
    'EFTPOS': 'EFTPOS',
    'VC/MC Dom.CR': 'VC/MC Dom.CR',
    'VC/MC Dom.DR': 'VC/MC Dom.DR',
    'VC/MC Prem.CR': 'VC/MC Prem.CR',
    'VC/MC Prem.DR': 'VC/MC Prem.DR',
    'VC/MC Int.CR': 'VC/MC Int.CR',
    'VC/MC Int.DR': 'VC/MC Int.DR',
}

# Query for TTV, MSF, COGS by payment method
query = """
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
    SUM(TTV) AS TTV,
    SUM(ABS(TOTAL_MSF)) AS MSF,
    SUM(ABS(TOTAL_ACQUIRER_FEE)) AS COGS
FROM dia_db.public.rba_base_model
GROUP BY 1
ORDER BY 1
"""
df = run_query(query)
ttv_lookup = {row['PAYMENT_METHOD']: row['TTV'] for _, row in df.iterrows()}
msf_lookup = {row['PAYMENT_METHOD']: row['MSF'] for _, row in df.iterrows()}
coa_lookup = {row['PAYMENT_METHOD']: row['COGS'] for _, row in df.iterrows()}

# Get base rates
base_msf_rate = {}
base_coa_rate = {}
for pm in payment_method_order:
    ttv = ttv_lookup.get(pm, 0)
    msf = msf_lookup.get(pm, 0)
    coa = coa_lookup.get(pm, 0)
    base_msf_rate[pm] = (msf / ttv * 10000) if ttv else 0
    base_coa_rate[pm] = (coa / ttv * 10000) if ttv else 0

# Get assumptions from session state
get_assumption = lambda key, pm: float(st.session_state.get(key, {}).get(pm, 0.0) or 0.0)

rows = []
for pm in payment_method_order:
    label = payment_method_labels[pm]
    ttv = ttv_lookup.get(pm, 0)
    msf = msf_lookup.get(pm, 0)
    coa = coa_lookup.get(pm, 0)
    base_msf = msf
    base_coa = coa
    base_msf_r = base_msf_rate[pm]
    base_coa_r = base_coa_rate[pm]
    new_msf_r = get_assumption('assumptions_msf', pm)
    new_coa_r = get_assumption('assumptions_coa', pm)
    new_msf = new_msf_r * ttv / 10000
    new_coa = new_coa_r * ttv / 10000
    base_gp = base_msf - base_coa
    new_gp = new_msf - new_coa
    rows.append({
        'Card type': label,
        'Base MSF Rate': base_msf_r,
        'Base MSF Amount': base_msf,
        'New MSF Rate': new_msf_r,
        'New MSF Amount': new_msf,
        'Base COA Rate': base_coa_r,
        'Base COA Amount': base_coa,
        'New COA Rate': new_coa_r,
        'New COA Amount': new_coa,
        'Base GP': base_gp,
        'New GP': new_gp
    })

df_gp = pd.DataFrame(rows)
amount_cols = ['Base MSF Amount', 'New MSF Amount', 'Base COA Amount', 'New COA Amount', 'Base GP', 'New GP']
rate_cols = ['Base MSF Rate', 'New MSF Rate', 'Base COA Rate', 'New COA Rate']

df_gp['Base MSF Rate'] = df_gp['Base MSF Rate'].map(lambda x: f"{x:.2f}")
df_gp['Base MSF Amount'] = df_gp['Base MSF Amount'].map(lambda x: f"${x:,.0f}")
df_gp['New MSF Rate'] = df_gp['New MSF Rate'].map(lambda x: f"{x:.2f}")
df_gp['New MSF Amount'] = df_gp['New MSF Amount'].map(lambda x: f"${x:,.0f}")
df_gp['Base COA Rate'] = df_gp['Base COA Rate'].map(lambda x: f"{x:.2f}")
df_gp['Base COA Amount'] = df_gp['Base COA Amount'].map(lambda x: f"${x:,.0f}")
df_gp['New COA Rate'] = df_gp['New COA Rate'].map(lambda x: f"{x:.2f}")
df_gp['New COA Amount'] = df_gp['New COA Amount'].map(lambda x: f"${x:,.0f}")
df_gp['Base GP'] = df_gp['Base GP'].map(lambda x: f"${x:,.0f}")
df_gp['New GP'] = df_gp['New GP'].map(lambda x: f"${x:,.0f}")

total_row = {
    'Card type': 'Total',
    'Base MSF Rate': '-',
    'Base MSF Amount': df_gp['Base MSF Amount'].replace('[\$,]', '', regex=True).astype(float).sum(),
    'New MSF Rate': '-',
    'New MSF Amount': df_gp['New MSF Amount'].replace('[\$,]', '', regex=True).astype(float).sum(),
    'Base COA Rate': '-',
    'Base COA Amount': df_gp['Base COA Amount'].replace('[\$,]', '', regex=True).astype(float).sum(),
    'New COA Rate': '-',
    'New COA Amount': df_gp['New COA Amount'].replace('[\$,]', '', regex=True).astype(float).sum(),
    'Base GP': df_gp['Base GP'].replace('[\$,]', '', regex=True).astype(float).sum(),
    'New GP': df_gp['New GP'].replace('[\$,]', '', regex=True).astype(float).sum(),
}
for col in amount_cols:
    total_row[col] = f"${total_row[col]:,.0f}"
for col in rate_cols:
    total_row[col] = '-'

df_gp = pd.concat([df_gp, pd.DataFrame([total_row])], ignore_index=True)

def highlight_base_new_cols(s):
    base_cols = ['Base MSF Rate', 'Base MSF Amount', 'Base COA Rate', 'Base COA Amount', 'Base GP']
    new_cols = ['New MSF Rate', 'New MSF Amount', 'New COA Rate', 'New COA Amount', 'New GP']
    color_base = 'background-color: #e3f2fd'
    color_new = 'background-color: #e8f5e9'
    return [
        color_base if col in base_cols else
        color_new if col in new_cols else
        '' for col in s.index
    ]

def bold_first_last_rows(row):
    if row.name == 0 or row.name == len(df_gp) - 1:
        return ['font-weight: bold'] * len(row)
    return [''] * len(row)

def highlight_total_row(row):
    if row.name == len(df_gp) - 1:
        return ['background-color: #ececec'] * len(row)
    return [''] * len(row)

styled_gp = df_gp.style.apply(highlight_base_new_cols, axis=1).apply(bold_first_last_rows, axis=1).apply(highlight_total_row, axis=1)
st.dataframe(styled_gp, use_container_width=True, hide_index=True) 