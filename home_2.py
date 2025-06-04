import streamlit as st
import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder

st.set_page_config(page_title="Oolio Group Dashboard 2", layout="wide")

# Sidebar navigation
st.sidebar.title("Navigation (v2)")
pages = {
    "TTV": "pages_2/TTV.py",
    "MSF": "pages_2/MSF.py",
    "COA": "pages_2/COA.py",
    "GP": "pages_2/GP.py",
    "GP Summary": "pages_2/GP_summary.py",
    "Model Business Unit": "pages_2/model_business_unit.py",
    "Model All": "pages_2/model_all.py",
    "Business Unit Model (with Card Types)": "pages_2/model_bu_2.py"
}
page = st.sidebar.radio("Go to", ["Dashboard"] + list(pages.keys()), key="page")

if page == "Dashboard":
    st.title("Oolio Group Dashboard 2")
    st.markdown("""
    Welcome to the new Oolio Group dashboard! Use the navigation links below to access the new pages:

    - [TTV](?page=TTV)
    - [MSF](?page=MSF)
    - [COA](?page=COA)
    - [GP](?page=GP)
    - [GP Summary](?page=GP%20Summary)
    - [Assumptions](?page=Assumptions)
    - [Model Business Unit](?page=Model%20Business%20Unit)
    - [Model All](?page=Model%20All)

    ---
    This dashboard provides advanced analytics and modeling for Oolio Group's business units and card types. Use the sidebar to navigate between pages.
    """)
else:
    with open(pages[page], encoding="utf-8") as f:
        code = f.read()
        exec(code, globals()) 