
 use role spcs_app_role;
 use warehouse app_wh;
 Use database detect_anomaly;
 Use schema public;

--- Ingest Anomaly For Inferencing

-- Prep Inference Data

Set days_of_data = 1; -- days
Set mean_temp = 100; -- degree
Set frequency = 1; -- min


--- Update Anomaly Set 1 for day=10
--- Here we are updating the target metric and all features

Update ms_review_for_anomaly_with_add_dimensions
set
measurement =  cte1.measurement,
voltage = cte1.voltage,
manufacturer =cte1.manufacturer,
weather_condition = cte1.weather_cond,
age = cte1.age,
anomaly_label=FALSE
from (
    With CTE1
    as (
        Select
        dateadd(minute,(row_number() over(order by seq) - 1), '2023-01-10 03:00:00') time,
        100 + normal(2,7,random(5)) measurement,
        (3 + uniform(3.0::float, 5::float, random())) voltage,
        (uniform(1,2, random())) manufacturer,
        (uniform(1,2, random())) weather_cond,
        (uniform(5,7, random())) Age
        from
        (Select seq4() as seq from table (generator(rowcount => 61)))
    ) 
    Select Time,measurement,name as manufacturer,condition as weather_cond ,voltage,age from CTE1 
    left join sensor_manufacturers sm
    on CTE1.manufacturer = sm.id
    left join weather_cond wc
    on CTE1.weather_cond = wc.id
) CTE1
where ms_review_for_anomaly_with_add_dimensions.ts = CTE1.time
and site_id = 'Site-A'
and sensor_id = 1;


Select * from ms_review_for_anomaly_with_add_dimensions 
where ts between '2023-01-10 03:00:00' and '2023-01-10 04:00:00' and site_id = 'Site-A'
and sensor_id = 1;



--- Update Anomaly Set 2 for day=10
--- Here we are updating the target metric and one features

Update ms_review_for_anomaly_with_add_dimensions
set
measurement =  cte1.measurement,
voltage = cte1.voltage
from (
    With CTE1
    as (
        Select
        dateadd(minute,(row_number() over(order by seq) - 1), '2023-01-10 15:00:00') time,
        100 + normal(2,7,random(5)) measurement,
        (3 + uniform(3.0::float, 5::float, random())) voltage,
        (uniform(1,2, random())) manufacturer,
        (uniform(1,2, random())) weather_cond,
        (uniform(5,7, random())) Age
        from
        (Select seq4() as seq from table (generator(rowcount => 61)))
    ) 
    Select Time,measurement,name as manufacturer,condition as weather_cond ,voltage,age from CTE1 
    left join sensor_manufacturers sm
    on CTE1.manufacturer = sm.id
    left join weather_cond wc
    on CTE1.weather_cond = wc.id
) CTE1
where ms_review_for_anomaly_with_add_dimensions.ts = CTE1.time
and site_id = 'Site-A'
and sensor_id = 2;


Select * from ms_review_for_anomaly_with_add_dimensions 
where ts between '2023-01-10 15:00:00' and '2023-01-10 16:00:00' and site_id = 'Site-A'
and sensor_id = 2;


-- Create an email integration

use role accountadmin;

CREATE or replace NOTIFICATION INTEGRATION send_email_notification_int
TYPE=EMAIL
ENABLED=TRUE;

GRANT USAGE ON INTEGRATION send_email_notification_int TO ROLE spcs_app_role;

use role spcs_app_role;

-- Create tables for saving and logging anomalies

CREATE OR REPLACE TABLE save_anomaly_detection (
  series variant,
  ts TIMESTAMP_NTZ, 
  y FLOAT, 
  forecast FLOAT, 
  lower_bound FLOAT, 
  upper_bound FLOAT,
  is_anomaly BOOLEAN, 
  percentile FLOAT, 
  distance FLOAT
);


CREATE OR REPLACE TABLE log_anomaly_detection (
ts timestamp,
measurement_start timestamp,
measurement_end timestamp,
no_of_anomalies int
);



-- Create a stored procedure to monitor and report anomalies. This stored procedure can be scheduled on demand. 
-- However for a quick demo we will run it for N number of times using a python program 

CREATE OR REPLACE PROCEDURE ms_monitoring_anomalies(start_time varchar, end_time varchar)
RETURNS integer NOT NULL
LANGUAGE SQL
AS
$$
DECLARE
  CREATE_SQL_STMT VARCHAR DEFAULT ''; 
  EMAIL_TXT VARCHAR;
  CNT INT;
  INS_SQL_STMT VARCHAR;
BEGIN

  CREATE_SQL_STMT := 'CREATE OR REPLACE VIEW TEST_DATA AS SELECT TS,[SITE_ID,SENSOR_ID] AS ITEM_ID,MEASUREMENT,ANOMALY_LABEL FROM MS_REVIEW_FOR_ANOMALY_WITH_ADD_DIMENSIONS WHERE TS BETWEEN '||''''||start_time||''' and '''||end_time||'''' ;  
  EXECUTE IMMEDIATE  CREATE_SQL_STMT; 
    
  CALL MS_DETECT_ANOMALY!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW','TEST_DATA'),
    SERIES_COLNAME => 'ITEM_ID',
    TIMESTAMP_COLNAME => 'TS',
    TARGET_COLNAME => 'MEASUREMENT',
    CONFIG_OBJECT => {'prediction_interval':0.999}
    );
  
  CREATE OR REPLACE TEMPORARY TABLE SAVE_ANOMALY_DETECTION_TEMP (SERIES,TS,Y,FORECAST,LOWER_BOUND,UPPER_BOUND,IS_ANOMALY,PERCENTILE,DISTANCE) 
  AS
  SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

  SELECT COUNT(1) INTO :CNT FROM SAVE_ANOMALY_DETECTION_TEMP WHERE IS_ANOMALY = TRUE;

  IF (:CNT >= 0) THEN

    INS_SQL_STMT := 'INSERT INTO LOG_ANOMALY_DETECTION (TS,MEASUREMENT_START,MEASUREMENT_END,NO_OF_ANOMALIES) VALUES (CURRENT_TIMESTAMP(), \''||TO_TIMESTAMP(start_time)||'\' , \''||TO_TIMESTAMP(end_time)||'\','||CNT||')';
    EXECUTE IMMEDIATE INS_SQL_STMT;

    IF (:CNT > 10) THEN

        INSERT INTO SAVE_ANOMALY_DETECTION (SERIES,TS,Y,FORECAST,LOWER_BOUND,UPPER_BOUND,IS_ANOMALY,PERCENTILE,DISTANCE)  
        SELECT * FROM SAVE_ANOMALY_DETECTION_TEMP;
  
        EMAIL_TXT := 'Anomalous Data Detected Between Measurent Timestamp:- ' ||start_time ||' and '||end_time||' with no of anomalies = '||cnt;
        CALL SYSTEM$SEND_EMAIL(
        'send_email_notification_int',
        'ritabrata.saha@snowflake.com',
        'Anomalous Measurement Detected',
        :EMAIL_TXT
         ); 
    END IF;
     
  END IF;

  RETURN CNT;
  
END;
$$;


 show snowflake.ml.anomaly_detection;


Select * from ms_review_for_anomaly_with_add_dimensions where ts>='2023-01-10 15:01:00';

