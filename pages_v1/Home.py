import streamlit as st

# Set page config
st.set_page_config(page_title="Oolio Group Feb-25  Adyen Advanced", layout="wide")

# Title
st.title("Home")

# Introduction
st.write("""
This dashboard provides insights into Oolio Group's transaction data for February 2025.
Use the sidebar or the quick links below to navigate between different views and models:
""")

st.header("Quick Navigation")
col1, col2 = st.columns(2)

with col1:
    st.subheader("TTV Analysis")
    st.write("View detailed breakdown of Total Transaction Value (TTV) across payment methods and business units.")
    st.page_link("pages/1_TTV by Card type.py", label="Go to TTV Analysis")
    st.subheader("MSF Analysis")
    st.write("Analyze Merchant Service Fees (MSF) across payment methods and business units.")
    st.page_link("pages/2_MSF by Card type.py", label="Go to MSF Analysis")
    st.subheader("COA Analysis")
    st.write("See Cost of Acquisition (COA) breakdown by payment method and business unit.")
    st.page_link("pages/3_COA by Card type.py", label="Go to COA Analysis")
    st.subheader("GP Analysis")
    st.write("Gross Profit (GP) by business unit and payment method, including MSF and COA.")
    st.page_link("pages/4_GP by Card type.py", label="Go to GP Analysis")

with col2:
    st.subheader("Assumptions")
    st.write("Set and adjust key assumptions for card mix, MSF, and COA rates. These drive the model scenarios.")
    st.page_link("pages/6_Assumptions.py", label="Go to Assumptions")
    st.subheader("Model TTV")
    st.write("Model the impact of new card mix assumptions on TTV by card type.")
    st.page_link("pages/7_Model TTV.py", label="Go to Model TTV")
    st.subheader("Model GP")
    st.write("Model the impact of new MSF and COA assumptions on GP by card type.")
    st.page_link("pages/8_Model GP.py", label="Go to Model GP")
    st.subheader("Summary")
    st.write("See a summary of all key financial metrics and model outputs.")
    st.page_link("pages/5_GP Summary.py", label="Go to Summary") 