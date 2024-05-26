
--- Review training data before ingesting anomalies

Select min(TS),max(TS) from ms_review_for_anomaly;

Select min(measurement),max(measurement) from ms_review_for_anomaly;

Select site_id,sensor_id,anomaly_label,month,count(day) from (
Select site_id,sensor_id,date_part(month,TS) month, date_part(day,TS) day,anomaly_label from ms_review_for_anomaly)
group by site_id,sensor_id,anomaly_label,month
order by site_id,sensor_id,anomaly_label,month;

--- Manual Inferencing, in case the customer is interest to review the code. Else lets use the app instead

TRUNCATE TABLE LOG_ANOMALY_DETECTION;
TRUNCATE TABLE save_anomaly_detection;

Select * from LOG_ANOMALY_DETECTION;
Select * from save_anomaly_detection;
Select * from save_anomaly_detection WHERE IS_ANOMALY = TRUE;


Create or replace view test_data as
Select ts,
[site_id,sensor_id] as item_id,
measurement,
anomaly_label 
from ms_review_for_anomaly
where ts between '2023-01-12 00:00:00' and '2023-01-12 23:59:00';


SELECT GET_DDL ('VIEW','test_data');
Select * from test_data;


-- By default, the value associated with the prediction_interval key is set to 0.99, which means that roughly 1% of the data is marked as anomalies. 
-- You can specify a value between 0 and 1:
-- To mark fewer observations as anomalies, specify a higher value for prediction_interval.
-- To mark more observations as anomalies, reduce the prediction_interval value.
--CONFIG_OBJECT => {'prediction_interval':0.995}


Use database detect_anomaly;
BEGIN
CALL MS_DETECT_ANOMALY!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW','TEST_DATA'),
    SERIES_COLNAME => 'ITEM_ID',
    TIMESTAMP_COLNAME => 'TS',
    TARGET_COLNAME => 'MEASUREMENT',
    CONFIG_OBJECT => {'prediction_interval':0.999}
    );

LET x := SQLID;
CREATE OR REPLACE TABLE save_anomaly_detection AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;


Select * from save_anomaly_detection where is_anomaly = True;

Select * from save_anomaly_detection where is_anomaly = True order by series,ts;

Select * from save_anomaly_detection where series[0] = 'Site-A' and series[1] = '1' ;

Select * from save_anomaly_detection where series[0] = 'Site-A' and series[1] = '2' ;

Select series[0] site_id,series[1] sensor_id,date_part(month,TS) month, date_part(day,TS) day from save_anomaly_detection where is_anomaly = True;

Set resultset = LAST_QUERY_ID();



Select 
trim(series[0],'""') site_id,
series[1] sensor_id,
ts,
y,
forecast,
is_anomaly
from save_anomaly_detection
where ts between '2023-01-12 06:59:00' and '2023-01-12 08:01:00'
order by site_id;



Select 
trim(series[0],'""') site_id,
series[1] sensor_id,
date_part(hour,ts) hour,
is_anomaly,
count(*)
from save_anomaly_detection
group by site_id,sensor_id,hour,is_anomaly;

Call ms_monitoring_anomalies('2023-01-12 00:00:00','2023-01-12 01:00:00');



