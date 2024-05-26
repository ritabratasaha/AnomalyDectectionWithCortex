import streamlit as st

st.set_page_config(
     page_title="Anomaly Monitor",
     page_icon="📊",
     layout="wide",
     initial_sidebar_state="expanded"
)


import os
from streamlit_option_menu import option_menu
from monitoringapp import *
from utils import *
from process_overview import pictorial_overview
from about import setup


# Snowflake connection setup. Build the session object
if "snowpark_session" not in st.session_state:
    session = create_session_object()
else:
    session = st.session_state.snowpark_session


with st.sidebar:
    choose_side_opt = option_menu("Sensor Monitor", ["Process Overview", "Demo Setup", "Sensor Monitor"],
                        icons=['house','tools','file-bar-graph-fill'],
                        menu_icon="cast", default_index=0,
                        styles={
                        "container": {"padding": "5!important", "background-color": "white","font-color": "#249dda"},
                        "icon": {"color": "#31c0e7", "font-size": "18px"}, 
                        "nav-link": {"font-size": "14px", "text-align": "left", "margin":"0px", "--hover-color": "white"},
                        "nav-link-selected": {"background-color": "7734f9"}
                        })


if choose_side_opt == "Sensor Monitor": 
    sensor_monitor()
elif choose_side_opt == "Process Overview":
    pictorial_overview()
elif choose_side_opt == "Demo Setup":
    setup()