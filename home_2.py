# Import comprehensive SSL patch first
# import ssl_patch

import streamlit as st
from config import get_snowflake_connection

st.set_page_config(
    page_title="Oolio Group Dashboard",
    page_icon="",
    layout="wide",
    initial_sidebar_state="collapsed"  # Better for mobile
)

# Mobile-friendly CSS
st.markdown("""
    <style>
    /* Make Streamlit main content area full width */
    .main .block-container {
        max-width: 100vw !important;
        padding-left: 2vw;
        padding-right: 2vw;
    }
    
    /* Mobile-friendly responsive design */
    @media (max-width: 768px) {
        .main .block-container {
            padding-left: 1vw;
            padding-right: 1vw;
        }
        
        /* Make sidebar more mobile-friendly */
        .css-1d391kg {
            width: 100% !important;
        }
        
        /* Adjust text sizes for mobile */
        .stMarkdown h1 {
            font-size: 1.5rem !important;
        }
        
        .stMarkdown h2 {
            font-size: 1.3rem !important;
        }
        
        .stMarkdown h3 {
            font-size: 1.1rem !important;
        }
        
        /* Make buttons more touch-friendly */
        .stButton > button {
            min-height: 44px !important;
            font-size: 16px !important;
        }
        
        /* Adjust multiselect for mobile */
        .stMultiSelect > div > div {
            min-height: 44px !important;
        }
        
        /* Make tables scrollable on mobile */
        .ag-theme-material {
            font-size: 12px !important;
        }
        
        /* Ensure proper touch targets */
        .stSelectbox > div > div {
            min-height: 44px !important;
        }
    }
    
    /* Fix for mobile viewport */
    @viewport {
        width: device-width;
        initial-scale: 1;
    }
    
    /* Ensure proper scaling on mobile */
    html {
        -webkit-text-size-adjust: 100%;
        -ms-text-size-adjust: 100%;
    }
    </style>
    """, unsafe_allow_html=True)

import pandas as pd
from st_aggrid import AgGrid, GridOptionsBuilder

# Sidebar navigation with mobile-friendly design
st.sidebar.title("Oolio Group Dashboard")
pages = {
    "TTV": "pages_2/TTV.py",
    # "TTV_2": "pages_2/TTV_2.py",
    "MSF ex GST": "pages_2/MSF.py",
    # "MSF_2": "pages_2/MSF_2.py",  # Removed from app
    "COA ex GST": "pages_2/COA.py",
    "GP ex GST": "pages_2/GP.py",
    "GP Summary": "pages_2/GP_summary.py",
    # "Model Business Unit": "pages_2/model_business_unit.py",
    # "Model All": "pages_2/model_all.py",
    "Model Table": "pages_2/model_bu_2.py",
    "Model Table V2": "pages_2/Model Table V2.py",
    "Model Table BU4 (Backup)": "pages_2/model_bu_4_backup.py",
    # "Business Unit Model": "pages_2/model_bu_3.py"
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