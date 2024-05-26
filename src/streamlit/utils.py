# Importing libraries
import streamlit as st
from snowflake.snowpark import Session
import snowflake.connector
import json
import subprocess
import pandas as pd
import os



def get_login_token():
  """
  Read the login token supplied automatically by Snowflake. These tokens
  are short lived and should always be read right before creating any new connection.
  """
  with open("/snowflake/session/token", "r") as f:
    return f.read()



def get_connection_params():
    """
    Construct Snowflake connection params from environment variables.
    """
    SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
    SNOWFLAKE_HOST = os.getenv("SNOWFLAKE_HOST")
    SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
    SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
    SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
    os.getenv("SNOWFLAKE_SCHEMA")

    if os.path.exists("/snowflake/session/token"):
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "host": SNOWFLAKE_HOST,
            "authenticator": "oauth",
            "token": get_login_token(),
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }
    else:
        return {
            "account": SNOWFLAKE_ACCOUNT,
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "role": 'spcs_app_role',
            "warehouse": SNOWFLAKE_WAREHOUSE,
            "database": SNOWFLAKE_DATABASE,
            "schema": SNOWFLAKE_SCHEMA
        }


# Creating a session into Snowflake
@st.cache_resource
def create_session_object() -> snowflake.connector.SnowflakeConnection:
    session = Session.builder.configs(get_connection_params()).create()
    #st.write('Session : ',session)
    return session


# Get Data Functions

def get_anomaly_logs() -> pd.DataFrame:
    df_anomaly_logs = session.sql('SELECT TS,MEASUREMENT_START,MEASUREMENT_END,NO_OF_ANOMALIES from DETECT_ANOMALY.PUBLIC.LOG_ANOMALY_DETECTION WHERE NO_OF_ANOMALIES>0;').to_pandas()
    #st.write(df_anomaly_logs)
    return df_anomaly_logs


def get_anomaly_sites(measurement_start, measurement_end) -> pd.DataFrame:
    stmt = 'Select site_id, sensor_id,count(is_anomaly) from ( \
            Select \
            trim(series[0],\'\') site_id, series[1] sensor_id,ts,y,forecast,is_anomaly \
            from DETECT_ANOMALY.PUBLIC.save_anomaly_detection where ts between \'' + measurement_start + '\' and \'' + measurement_end + '\'' + ') \
            where is_anomaly=True group by site_id, sensor_id order by site_id, sensor_id;'
    #print (stmt)
    df_anomaly_sites = session.sql(stmt).to_pandas()
    return df_anomaly_sites


def get_anomalies(site_id,sensor_id,measurement_start,measurement_end):
    stmt = 'Select \
            trim(series[0],\'\') site_id, \
            series[1] sensor_id ,\
            ts,\
            y readings,\
            forecast,is_anomaly \
            from DETECT_ANOMALY.PUBLIC.save_anomaly_detection where site_id = \'' + site_id + '\' and sensor_id = \'' + sensor_id \
            + '\' and ts between \'' + measurement_start + '\' and \'' + measurement_end + '\''
    #print (stmt)
    df_anomaly = session.sql(stmt).to_pandas()
    return df_anomaly
            

def get_contributors(site_id,sensor_id,measurement_start,measurement_end):
    stmt = 'WITH input AS (\
            SELECT\
                {\
                \'manufacturer\': tab.manufacturer,\
                \'weathercondition\': tab.weather_condition\
                }\
                AS categorical_dimensions,\
                {\
                \'voltage\': tab.voltage,\
                \'age\': tab.age \
                }\
                AS continuous_dimensions,\
                tab.measurement,\
                IFF( (ts between \'' + measurement_start + '\' and \'' + measurement_end + '\') and site_id = \''+ site_id + '\' and sensor_id = \''+ sensor_id + '\', TRUE, FALSE) AS label\
            FROM ms_review_for_anomaly_with_add_dimensions tab\
            ) \
            SELECT contributor,surprise,relative_change from input, TABLE(\
            SNOWFLAKE.ML.TOP_INSIGHTS(\
                input.categorical_dimensions,\
                input.continuous_dimensions,\
                input.measurement,\
                input.label\
            )\
            OVER (PARTITION BY 0)\
            ) res ORDER BY res.surprise DESC limit 20;'
    
    print (stmt)
    df_contributors = session.sql(stmt).to_pandas()
    return df_contributors


# Snowflake connection setup. Build the session object
if "snowpark_session" not in st.session_state:
    session = create_session_object()
else:
    session = st.session_state.snowpark_session
