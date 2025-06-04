import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
from config import SNOWFLAKE_CONFIG

st.set_page_config(page_title="Oolio Group Feb-25  Adyen Advanced", layout="wide")
st.title("Assumptions")

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

# Query for TTV by payment method and get the 'Total' business unit, '% of TTV' row
query = """
WITH base_data AS (
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
)
SELECT PAYMENT_METHOD, TTV FROM base_data
"""
df = run_query(query)
total_ttv = df['TTV'].sum()
benchmarks = {}
for pm in payment_method_order:
    if pm in df['PAYMENT_METHOD'].values:
        ttv = df[df['PAYMENT_METHOD'] == pm]['TTV'].sum()
        benchmarks[pm] = (ttv / total_ttv * 100) if total_ttv else 0
    else:
        benchmarks[pm] = 0.0

if 'assumptions' not in st.session_state:
    st.session_state['assumptions'] = {pm: None for pm in payment_method_order}
else:
    # Add any missing keys (e.g., after renaming card types)
    for pm in payment_method_order:
        if pm not in st.session_state['assumptions']:
            st.session_state['assumptions'][pm] = None
# Ensure all values are float or None
for pm in payment_method_order:
    if not (isinstance(st.session_state['assumptions'][pm], float) or st.session_state['assumptions'][pm] is None):
        st.session_state['assumptions'][pm] = 0.0

# Query for GTV, MSF, COGS by payment method
query_rates = """
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
    SUM(TTV) AS GTV,
    SUM(ABS(TOTAL_MSF)) AS MSF,
    SUM(ABS(TOTAL_ACQUIRER_FEE)) AS COGS
FROM dia_db.public.rba_base_model
GROUP BY 1
ORDER BY 1
"""
df_rates = run_query(query_rates)
# Build lookup for GTV, MSF, COGS by card type
rate_lookup = {row['PAYMENT_METHOD']: row for _, row in df_rates.iterrows()}
# Calculate base MSF Rate and COA Rate
base_msf_rate = {}
base_coa_rate = {}
for pm in payment_method_order:
    row = rate_lookup.get(pm, {'GTV': 0, 'MSF': 0, 'COGS': 0})
    gtv = row['GTV']
    msf = row['MSF']
    cogs = row['COGS']
    base_msf_rate[pm] = (msf / gtv * 10000) if gtv else 0
    base_coa_rate[pm] = (cogs / gtv * 10000) if gtv else 0
# Session state for msf and coa assumptions
if 'assumptions_msf' not in st.session_state:
    st.session_state['assumptions_msf'] = {pm: None for pm in payment_method_order}
else:
    for pm in payment_method_order:
        if pm not in st.session_state['assumptions_msf']:
            st.session_state['assumptions_msf'][pm] = None
if 'assumptions_coa' not in st.session_state:
    st.session_state['assumptions_coa'] = {pm: None for pm in payment_method_order}
else:
    for pm in payment_method_order:
        if pm not in st.session_state['assumptions_coa']:
            st.session_state['assumptions_coa'][pm] = None
# Ensure all values are float or None
for pm in payment_method_order:
    if not (isinstance(st.session_state['assumptions_msf'][pm], float) or st.session_state['assumptions_msf'][pm] is None):
        st.session_state['assumptions_msf'][pm] = 0.0
    if not (isinstance(st.session_state['assumptions_coa'][pm], float) or st.session_state['assumptions_coa'][pm] is None):
        st.session_state['assumptions_coa'][pm] = 0.0

st.markdown("""
<style>
.cardmix-table-header {
    background-color: #b3e5fc;
    font-weight: bold;
    text-align: center;
    padding: 8px;
    border-radius: 4px;
    margin-bottom: 0px;
}
.cardmix-table-row {
    background-color: #f7fafd;
    border-radius: 4px;
    margin-bottom: 2px;
    padding-top: 2px;
    padding-bottom: 2px;
}
.cardmix-assump-row {
    display: flex;
    align-items: center;
    gap: 4px;
}
.cardmix-btn {
    margin-left: 2px;
    margin-right: 2px;
    padding: 0 8px;
    height: 28px;
    font-size: 18px;
}
</style>
""", unsafe_allow_html=True)

st.write('### Assumptions Table')

# Header row
header_cols = st.columns([1.3, 1, 2, 1, 2, 1, 2])
header_cols[0].markdown('<div style="background-color:#e3e3e3;text-align:center;font-weight:bold;border-radius:4px;">Card Type</div>', unsafe_allow_html=True)
header_cols[1].markdown('<div style="background-color:#b3e5fc;text-align:center;font-weight:bold;border-radius:4px;">Card Mix<br>Benchmark</div>', unsafe_allow_html=True)
header_cols[2].markdown('<div style="background-color:#b3e5fc;text-align:center;font-weight:bold;border-radius:4px;">Card Mix<br>Assumption (%)</div>', unsafe_allow_html=True)
header_cols[3].markdown('<div style="background-color:#dcedc8;text-align:center;font-weight:bold;border-radius:4px;">MSF Rate<br>Benchmark</div>', unsafe_allow_html=True)
header_cols[4].markdown('<div style="background-color:#dcedc8;text-align:center;font-weight:bold;border-radius:4px;">MSF Rate<br>Assumption</div>', unsafe_allow_html=True)
header_cols[5].markdown('<div style="background-color:#f8bbd0;text-align:center;font-weight:bold;border-radius:4px;">COA Rate<br>Benchmark</div>', unsafe_allow_html=True)
header_cols[6].markdown('<div style="background-color:#f8bbd0;text-align:center;font-weight:bold;border-radius:4px;">COA Rate<br>Assumption</div>', unsafe_allow_html=True)

rerun_needed = False

for pm in payment_method_order:
    row = st.columns([1.3, 1, 2, 1, 2, 1, 2])
    # Card Type
    row[0].markdown(f'<div style="background:#f7fafd;text-align:center;">{payment_method_labels[pm]}</div>', unsafe_allow_html=True)
    # Card Mix
    bench = benchmarks.get(pm, 0.0)
    row[1].markdown(f'<div style="background:#f7fafd;text-align:center;">{bench:.2f}%</div>', unsafe_allow_html=True)
    with row[2]:
        subcols = st.columns([2, 1, 1])
        val = subcols[0].number_input(" ", min_value=0.0, max_value=100.0, value=float(st.session_state['assumptions'][pm] or 0.0), step=0.01, key=f"{pm}_assumption", label_visibility="collapsed")
        minus = subcols[1].button("-", key=f"{pm}_minus")
        plus = subcols[2].button("+", key=f"{pm}_plus")
        if minus:
            st.session_state['assumptions'][pm] = max(0.0, val - 0.01)
            rerun_needed = True
        elif plus:
            st.session_state['assumptions'][pm] = min(100.0, val + 0.01)
            rerun_needed = True
        else:
            st.session_state['assumptions'][pm] = val
    # MSF Rate
    msf_bench = base_msf_rate.get(pm, 0.0)
    row[3].markdown(f'<div style="background:#f7fafd;text-align:center;">{msf_bench:.2f}</div>', unsafe_allow_html=True)
    with row[4]:
        subcols = st.columns([2, 1, 1])
        msf_val = subcols[0].number_input("  ", min_value=0.0, max_value=1000.0, value=float(st.session_state['assumptions_msf'][pm] or 0.0), step=0.01, key=f"{pm}_msf_assumption", label_visibility="collapsed")
        msf_minus = subcols[1].button("-", key=f"{pm}_msf_minus")
        msf_plus = subcols[2].button("+", key=f"{pm}_msf_plus")
        if msf_minus:
            st.session_state['assumptions_msf'][pm] = max(0.0, msf_val - 0.01)
            rerun_needed = True
        elif msf_plus:
            st.session_state['assumptions_msf'][pm] = min(1000.0, msf_val + 0.01)
            rerun_needed = True
        else:
            st.session_state['assumptions_msf'][pm] = msf_val
    # COA Rate
    coa_bench = base_coa_rate.get(pm, 0.0)
    row[5].markdown(f'<div style="background:#f7fafd;text-align:center;">{coa_bench:.2f}</div>', unsafe_allow_html=True)
    with row[6]:
        subcols = st.columns([2, 1, 1])
        coa_val = subcols[0].number_input("   ", min_value=0.0, max_value=1000.0, value=float(st.session_state['assumptions_coa'][pm] or 0.0), step=0.01, key=f"{pm}_coa_assumption", label_visibility="collapsed")
        coa_minus = subcols[1].button("-", key=f"{pm}_coa_minus")
        coa_plus = subcols[2].button("+", key=f"{pm}_coa_plus")
        if coa_minus:
            st.session_state['assumptions_coa'][pm] = max(0.0, coa_val - 0.01)
            rerun_needed = True
        elif coa_plus:
            st.session_state['assumptions_coa'][pm] = min(1000.0, coa_val + 0.01)
            rerun_needed = True
        else:
            st.session_state['assumptions_coa'][pm] = coa_val

if rerun_needed:
    st.rerun() 