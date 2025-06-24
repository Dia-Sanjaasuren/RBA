import streamlit as st
import pandas as pd
import numpy as np
import snowflake.connector
import os

st.set_page_config(page_title="Merchant Model", layout="wide")
st.title("Merchant Level Analysis")

CARD_TYPES = [
    'AMEX', 'EFTPOS', 'VC/MC Domestic Credit', 'VC/MC Domestic Debit',
    'VC/MC Premium Credit', 'VC/MC Premium Debit', 'VC/MC Int.Credit', 'VC/MC Int.Debit'
]

# Load merchant data
@st.cache_data(ttl=3600)
def load_merchant_data():
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA'),
            insecure_mode=True
        )
        query = """
        WITH grouped AS (
            SELECT
                display_name,
                merchant_account,
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
                ttv,
                total_msf,
                total_acquirer_fee
            FROM dia_db.public.rba_base_model
        )
        SELECT
            display_name,
            merchant_account,
            PAYMENT_METHOD,
            SUM(ttv) AS ttv,
            SUM(total_msf) AS msf,
            SUM(total_acquirer_fee) AS acquirer_fee
        FROM grouped
        GROUP BY display_name, merchant_account, PAYMENT_METHOD
        ORDER BY merchant_account;
        """
        df = pd.read_sql(query, conn)
        conn.close()
        df.columns = df.columns.str.lower()
        return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return pd.DataFrame({
            'display_name': [],
            'merchant_account': [],
            'payment_method': [],
            'ttv': [],
            'msf': [],
            'acquirer_fee': []
        })

df = load_merchant_data()

# Merchant Selection
st.write("### Select Merchants")
merchant_options = df['display_name'].unique()
selected_merchants = st.multiselect(
    "Choose merchants to analyze",
    options=merchant_options,
    default=[]
)

# Build assumptions table per (merchant, card type)
assumptions_rows = []
if selected_merchants:
    filtered_df = df[df['display_name'].isin(selected_merchants)]
    for merchant in selected_merchants:
        merchant_df = filtered_df[filtered_df['display_name'] == merchant]
        merchant_total_ttv = merchant_df['ttv'].sum()
        for card_type in CARD_TYPES:
            card_df = merchant_df[merchant_df['payment_method'] == card_type]
            ttv = card_df['ttv'].sum()
            msf = card_df['msf'].sum()
            coa = card_df['acquirer_fee'].sum()
            # Calculate benchmarks at merchant level
            card_mix_benchmark = (ttv / merchant_total_ttv * 100) if merchant_total_ttv > 0 else 0.0
            msf_rate_benchmark = (abs(msf) / ttv * 10000) if ttv > 0 else 0.0
            coa_rate_benchmark = (abs(coa) / ttv * 10000) if ttv > 0 else 0.0
            # Try to persist assumptions if already in session state
            key = f"{merchant}__{card_type}"
            if 'assumptions_dict' not in st.session_state:
                st.session_state['assumptions_dict'] = {}
            prev = st.session_state['assumptions_dict'].get(key, {})
            assumptions_rows.append({
                'Merchant': merchant,
                'Card Type': card_type,
                'Card Mix Benchmark': card_mix_benchmark,
                'Card Mix Assumption (%)': prev.get('Card Mix Assumption (%)', 0.0),
                'MSF Rate Benchmark': msf_rate_benchmark,
                'MSF Rate Assumption': prev.get('MSF Rate Assumption', 0.0),
                'COA Rate Benchmark': coa_rate_benchmark,
                'COA Rate Assumption': prev.get('COA Rate Assumption', 0.0),
                'key': key
            })
else:
    # If no merchants selected, show empty table for all merchants/card types
    assumptions_rows = []

assumptions_df = pd.DataFrame(assumptions_rows)

# Show unified assumptions table
st.write("### Assumptions Table")
st.markdown('<div style="background-color:#f5f5dc;padding:8px 16px;border-radius:6px;margin-bottom:8px;">'
            '<b>Note:</b> The <span style="color:#b8860b;">Assumption</span> columns are editable. Please enter your assumptions in these beige columns.</div>', unsafe_allow_html=True)
if not assumptions_df.empty:
    edited_table = st.data_editor(
        assumptions_df.drop(columns=['key']),
        use_container_width=True,
        num_rows="fixed",
        column_config={
            "Merchant": st.column_config.TextColumn(disabled=True),
            "Card Type": st.column_config.TextColumn(disabled=True),
            "Card Mix Benchmark": st.column_config.NumberColumn(disabled=True, format="%.2f%%"),
            "MSF Rate Benchmark": st.column_config.NumberColumn(disabled=True, format="%.2f"),
            "COA Rate Benchmark": st.column_config.NumberColumn(disabled=True, format="%.2f"),
            "Card Mix Assumption (%)": st.column_config.NumberColumn(format="%.2f"),
            "MSF Rate Assumption": st.column_config.NumberColumn(format="%.2f"),
            "COA Rate Assumption": st.column_config.NumberColumn(format="%.2f"),
        }
    )
    # Save edits to session state
    for i, row in edited_table.iterrows():
        key = assumptions_df.iloc[i]['key']
        st.session_state['assumptions_dict'][key] = {
            'Card Mix Assumption (%)': row['Card Mix Assumption (%)'],
            'MSF Rate Assumption': row['MSF Rate Assumption'],
            'COA Rate Assumption': row['COA Rate Assumption']
        }
else:
    st.info("Please select at least one merchant to view analysis.")

# --- Model TTV Table ---
def model_ttv_table(assumptions_df, df):
    rows = []
    merchant_groups = assumptions_df.groupby('Merchant')
    for merchant, group in merchant_groups:
        # Get merchant's total ttv from original df
        merchant_df = df[df['display_name'] == merchant]
        merchant_total_ttv = merchant_df['ttv'].sum()
        for _, row in group.iterrows():
            card_type = row['Card Type']
            base_percent = row['Card Mix Benchmark']
            base_ttv = (base_percent / 100) * merchant_total_ttv if merchant_total_ttv > 0 else 0
            new_percent = row['Card Mix Assumption (%)']
            new_ttv = (new_percent / 100) * merchant_total_ttv if merchant_total_ttv > 0 else 0
            rows.append({
                'Merchant': merchant,
                'Card Type': card_type,
                'Base percent': base_percent,
                'Base TTV': base_ttv,
                'New percent': new_percent,
                'New TTV': new_ttv
            })
    df_ttv = pd.DataFrame(rows)
    # Add total row
    if not df_ttv.empty:
        total_row = {
            'Merchant': 'Total',
            'Card Type': '',
            'Base percent': df_ttv['Base percent'].sum(),
            'Base TTV': df_ttv['Base TTV'].sum(),
            'New percent': df_ttv['New percent'].sum(),
            'New TTV': df_ttv['New TTV'].sum()
        }
        df_ttv = pd.concat([df_ttv, pd.DataFrame([total_row])], ignore_index=True)
    # Format
    for col in ['Base percent', 'New percent']:
        df_ttv[col] = df_ttv[col].map(lambda x: f"{x:.2f}" if x != '' else '')
    for col in ['Base TTV', 'New TTV']:
        df_ttv[col] = df_ttv[col].map(lambda x: f"{x:,.0f}" if x != '' else '')
    return df_ttv

# --- Model GP Table ---
def model_gp_table(assumptions_df, df):
    rows = []
    for _, row in assumptions_df.iterrows():
        merchant = row['Merchant']
        card_type = row['Card Type']
        # Get ttv, msf, coa from original df
        card_df = df[(df['display_name'] == merchant) & (df['payment_method'] == card_type)]
        ttv = card_df['ttv'].sum()
        msf = abs(card_df['msf'].sum())  # Always positive
        coa = abs(card_df['acquirer_fee'].sum())  # Always positive
        # Base rates
        base_msf_rate = (msf / ttv * 10000) if ttv > 0 else 0.0
        base_coa_rate = (coa / ttv * 10000) if ttv > 0 else 0.0
        # New rates from assumptions
        new_msf_rate = row['MSF Rate Assumption']
        new_coa_rate = row['COA Rate Assumption']
        # Amounts
        base_msf_amt = msf
        base_coa_amt = coa
        new_msf_amt = (new_msf_rate / 10000) * ttv if ttv > 0 else 0.0
        new_coa_amt = (new_coa_rate / 10000) * ttv if ttv > 0 else 0.0
        base_gp_amt = msf - coa
        new_gp_amt = new_msf_amt - new_coa_amt
        rows.append({
            'Merchant': merchant,
            'Card Type': card_type,
            'Base MSF rate': base_msf_rate,
            'Base MSF Amount': base_msf_amt,
            'New MSF rate': new_msf_rate,
            'New MSF Amount': new_msf_amt,
            'Base COA rate': base_coa_rate,
            'Base COA Amount': base_coa_amt,
            'New COA rate': new_coa_rate,
            'New COA Amount': new_coa_amt,
            'Base GP Amount': base_gp_amt,
            'New GP Amount': new_gp_amt
        })
    df_gp = pd.DataFrame(rows)
    # Add total row for amount columns
    if not df_gp.empty:
        total_row = {
            'Merchant': 'Total',
            'Card Type': '',
            'Base MSF rate': '',
            'Base MSF Amount': df_gp['Base MSF Amount'].sum(),
            'New MSF rate': '',
            'New MSF Amount': df_gp['New MSF Amount'].sum(),
            'Base COA rate': '',
            'Base COA Amount': df_gp['Base COA Amount'].sum(),
            'New COA rate': '',
            'New COA Amount': df_gp['New COA Amount'].sum(),
            'Base GP Amount': df_gp['Base GP Amount'].sum(),
            'New GP Amount': df_gp['New GP Amount'].sum()
        }
        df_gp = pd.concat([df_gp, pd.DataFrame([total_row])], ignore_index=True)
    # Format
    for col in ['Base MSF rate', 'New MSF rate', 'Base COA rate', 'New COA rate']:
        df_gp[col] = df_gp[col].map(lambda x: f"{x:.2f}" if x != '' else '')
    for col in ['Base MSF Amount', 'New MSF Amount', 'Base COA Amount', 'New COA Amount', 'Base GP Amount', 'New GP Amount']:
        df_gp[col] = df_gp[col].map(lambda x: f"{x:,.0f}" if x != '' else '')
    return df_gp

# --- Display Model Tables ---
if not assumptions_df.empty:
    st.write("### Model TTV Table")
    ttv_model = model_ttv_table(assumptions_df, df)
    def ttv_style(row):
        base_cols = ['Base percent', 'Base TTV']
        new_cols = ['New percent', 'New TTV']
        style = [
            'background-color: #e3f2fd' if col in base_cols else
            'background-color: #e8f5e9' if col in new_cols else ''
            for col in row.index
        ]
        if row['Merchant'] == 'Total':
            style = ['font-weight: bold; color: black; background-color: #fffde7' for _ in row]
        return style
    st.dataframe(ttv_model.style.apply(ttv_style, axis=1), use_container_width=True, hide_index=True)

    st.write("### Model GP Table")
    gp_model = model_gp_table(assumptions_df, df)
    def gp_style(row):
        base_cols = ['Base MSF rate', 'Base MSF Amount', 'Base COA rate', 'Base COA Amount', 'Base GP Amount']
        new_cols = ['New MSF rate', 'New MSF Amount', 'New COA rate', 'New COA Amount', 'New GP Amount']
        style = [
            'background-color: #e3f2fd' if col in base_cols else
            'background-color: #e8f5e9' if col in new_cols else ''
            for col in row.index
        ]
        if row['Merchant'] == 'Total':
            style = ['font-weight: bold; color: black; background-color: #fffde7' for _ in row]
        return style
    st.dataframe(gp_model.style.apply(gp_style, axis=1), use_container_width=True, hide_index=True)

# (You can now use st.session_state['assumptions_dict'] for downstream modeling/calculations) 