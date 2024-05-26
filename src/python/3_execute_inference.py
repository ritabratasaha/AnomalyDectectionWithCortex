
# Importing libraries
from snowflake.snowpark import Session
import snowflake.connector
import json
#import datetime 
from datetime import datetime ,timedelta
import time
import os

def create_session_object():
    connection_parameters = {
        'account' : os.getenv('SNOWFLAKE_ACCOUNT'),
        'role' : os.getenv('SNOWFLAKE_ROLE'),
        'user' : os.getenv('SNOWFLAKE_USER'),
        'password' : os.getenv('SNOWFLAKE_PASSWORD'), 
        'database' : os.getenv('SNOWFLAKE_DATABASE'),
        'schema' : os.getenv('SNOWFLAKE_SCHEMA'),
        'warehouse' : os.getenv('SNOWFLAKE_WAREHOUSE'),
        'client_session_keep_alive': True
        }
    # Let's create a session on Snowflake
    connection = snowflake.connector.connect(**connection_parameters)
    session = Session.builder.configs({"connection": connection}).create()
    print(session.sql('select current_warehouse(), current_database(), current_schema(), current_role()').collect())
    # stmt = 'use warehouse ' + str(os.getenv('SNOWFLAKE_WAREHOSE'))
    # session.sql(stmt).collect()
    return session 


start_time_str = '2023-01-10 01:00:00'
start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
target_iterations = 20
counter_iterations = 0


session = create_session_object()
while (counter_iterations <= target_iterations):
    time_change = timedelta(hours=1) 
    end_time = start_time + time_change

    measurement_start = start_time
    measurement_end = end_time

    # Five mins overlap
    measurement_start = str(datetime.strptime(str(start_time), '%Y-%m-%d %H:%M:%S') - timedelta(minutes=5))
    measurement_end = str(datetime.strptime(str(end_time), '%Y-%m-%d %H:%M:%S') + timedelta(minutes=5))

    print ("Startime = {0} and Endtime = {1}".format(start_time,end_time))
    stmt= 'Call ms_monitoring_anomalies(\'' + str(measurement_start) + '\',\'' + str(measurement_end) + '\')'
    print(stmt)
    session.sql(stmt).collect()
    counter_iterations += 1
    start_time=end_time
    time.sleep(5)

