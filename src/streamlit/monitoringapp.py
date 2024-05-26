import streamlit as st
from streamlit_option_menu import option_menu
from st_aggrid import AgGrid,ColumnsAutoSizeMode,GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
from streamlit_echarts import st_echarts
from utils import *
from datetime import datetime,timedelta



#Snowflake connection setup. Build the session object

if "snowpark_session" not in st.session_state:
    session = create_session_object()
else:
    session = st.session_state.snowpark_session



def sensor_monitor():

    # Make connection to Snowflake and cache it

    st.title(f":blue[ Investigate Reported Anomalies : ]")
    st.subheader(f":blue[ Select An Interval For Investigation : ]")
    st.info('There is a 5 min overlap of anomaly detection window', icon="ℹ️")

    custom_css = {
        ".ag-header-cell-text": {"font-size": "18px", 'text-overflow': 'revert;', 'font-weight': 900},
        ".ag-theme-streamlit": {'transform': "scale(0.8)", "transform-origin": '0 0'},
        ".ag-row-hover": {"background-color": "orange"}
        }
    
    df=get_anomaly_logs()
    if df.empty:
        st.success("No anomalies have been reported so far !!")
    else:
        #print(df.empty)
        gd = GridOptionsBuilder.from_dataframe(df)
        gd.configure_default_column(cellStyle={'color': 'blue', 'font-size': '18px'},  wrapHeaderText=True, autoHeaderHeight=True)
        gd.configure_selection(selection_mode='single',use_checkbox=True)
        gridoptions = gd.build()
        selection = AgGrid(data=df,
            gridOptions=gridoptions,
            width=500,
            update_mode=GridUpdateMode.SELECTION_CHANGED,
            columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW, 
            custom_css=custom_css,
            allow_unsafe_jscode=True)
        
        if selection["selected_rows"] != []:
            ts = (selection["selected_rows"][0]["TS"])
            measurement_start = (selection["selected_rows"][0]["MEASUREMENT_START"])
            measurement_end = (selection["selected_rows"][0]["MEASUREMENT_END"])

            # measurement_start = str(datetime.strptime(measurement_start, '%Y-%m-%dT%H:%M:%S') - timedelta(minutes=60))
            # measurement_end = str(datetime.strptime(measurement_end, '%Y-%m-%dT%H:%M:%S') + timedelta(minutes=60))
            #print(ts,measurement_start,measurement_end)
            
            st.subheader(f":blue[ Select A Site/Sensor For Visualization : ]")
            st.write('\n')
            df_anomaly_sites=get_anomaly_sites(measurement_start,measurement_end)
            gd = GridOptionsBuilder.from_dataframe(df_anomaly_sites)
            gd.configure_default_column(cellStyle={'color': 'blue', 'font-size': '20px'},  wrapHeaderText=True, autoHeaderHeight=True)
            gd.configure_selection(selection_mode='single',use_checkbox=True)
            gridoptions = gd.build()
            selection = AgGrid(data=df_anomaly_sites,
                gridOptions=gridoptions,
                width=500,
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                columns_auto_size_mode=ColumnsAutoSizeMode.FIT_ALL_COLUMNS_TO_VIEW, 
                custom_css=custom_css,
                allow_unsafe_jscode=True)
            
            if selection["selected_rows"] != []:
                selected_site = selection["selected_rows"][0]["SITE_ID"]
                selected_sensor = selection["selected_rows"][0]["SENSOR_ID"]
                print(measurement_start,measurement_end)
                df_sensor_anomaly = get_anomalies(selected_site,selected_sensor,measurement_start,measurement_end)
                st.subheader(":blue[ Actual Vs Forecasted Measurements : ]")
                st.line_chart(
                        df_sensor_anomaly, x="TS", y=["READINGS", "FORECAST"], color=["#00ff62", "#FF0000"]  # Optional
                        )
                df_contributors = get_contributors(selected_site,selected_sensor,measurement_start,measurement_end)
                
                st.subheader(":blue[ Top Features Contributing To The Anomaly : ]")
                st.write('\n')
                st.dataframe(df_contributors,width=1100)