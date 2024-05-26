use role accountadmin;

create role if not exists spcs_app_role;

grant create integration on account to role spcs_app_role;
grant create compute pool on account to role spcs_app_role;
grant create warehouse on account to role spcs_app_role;
grant create database on account to role spcs_app_role;
grant usage on integration allow_all_eai to role spcs_app_role;
grant bind service endpoint on account to role spcs_app_role;


declare
    username varchar;
    stmt varchar;
begin
    Select current_user() into :username;
    stmt := 'GRANT ROLE spcs_app_role TO USER ' || :username;
    execute immediate stmt;
    return 'role assigned';
end;


use role spcs_app_role;

create warehouse if not exists app_wh;
Use warehouse app_wh;

Create database if not exists detect_anomaly;
Use database detect_anomaly;
Create database if not exists detect_anomaly;

Use database detect_anomaly;


--- Create static reference data
Create or replace table sensor_manufacturers(
Id int,
Name Varchar(100)
);

Insert into sensor_manufacturers values(1,'Panasonic Corporation');
Insert into sensor_manufacturers values(2,'Qualcomm Technologies');
Insert into sensor_manufacturers values(3,'STMicroelectronics');
Insert into sensor_manufacturers values(4,'Sony Corporation');
Insert into sensor_manufacturers values(5,'TE Connectivity');
Insert into sensor_manufacturers values(6,'Texas Instruments');
Insert into sensor_manufacturers values(7,'Siemens');
Insert into sensor_manufacturers values(8,'Amphenol Corporation');
Insert into sensor_manufacturers values(9,'Dwyer Instruments, LLC');
Insert into sensor_manufacturers values(10,'Bosch Sensortec');
Insert into sensor_manufacturers values(11,'Honeywell International');
Insert into sensor_manufacturers values(12,'Sensirion AG');


Create or replace table weather_cond(
Id int,
Condition Varchar(100)
);

Insert into weather_cond values(1,'Windy');
Insert into weather_cond values(2,'Rainy');
Insert into weather_cond values(3,'Humid');
Insert into weather_cond values(4,'Snow');
Insert into weather_cond values(5,'Cold');
Insert into weather_cond values(6,'Cloudy');
Insert into weather_cond values(7,'Storm');
Insert into weather_cond values(8,'Hot');


-- Prepare Training Data ( 10 days of labeled training data )

Create or replace table ms_review_for_anomaly_with_add_dimensions
(   ts timestamp,
    site_id varchar,
    sensor_id int,
    row_id int,
    measurement float,
    anomaly_label boolean,
    manufacturer varchar(100),
    weather_condition varchar(100),
    voltage float,
    age int
);

Set days_of_data = 10; -- days
Set mean_temp = 100; -- degree
Set frequency = 1; -- min



--- Generating Data for 10 Days for Site -A
Insert into ms_review_for_anomaly_with_add_dimensions
With CTE1
as (
Select 
dateadd(minute,(row_number() over(order by seq) - 1), '2023-01-01 00:00:00') time,
'Site-A' as site_id,
'1' as sensor_id,
row_number() over(order by seq) row_id,
$mean_temp + normal(0,1,random(1)) measurement,
False as anomaly_label,
(uniform(1, 12, random())) manufacturer,
(uniform(1, 8, random())) weather_cond,
(uniform(1.5::float, 3::float, random())) Voltage,
(uniform(1, 5, random())) Age,
from
(Select seq4() as seq from table (generator(rowcount => 1 + $days_of_data*24*60/$frequency)))
)
Select Time,site_id,sensor_id,row_id,measurement,anomaly_label,name as manufacturer,condition as weather_cond ,voltage,age from CTE1 
left join sensor_manufacturers sm
on CTE1.manufacturer = sm.id
left join weather_cond wc
on CTE1.weather_cond = wc.id;


--- Generating Data for 30 Days for Site -A
Insert into ms_review_for_anomaly_with_add_dimensions
With CTE1
as (
Select 
dateadd(minute,(row_number() over(order by seq) - 1), '2023-01-01 00:00:00') time,
'Site-A' as site_id,
'2' as sensor_id,
row_number() over(order by seq) row_id,
$mean_temp + normal(0,1,random(1)) measurement,
False as anomaly_label,
(uniform(1, 12, random())) manufacturer,
(uniform(1, 8, random())) weather_cond,
(uniform(1.5::float, 3::float, random())) Voltage,
(uniform(1, 5, random())) Age,
from
(Select seq4() as seq from table (generator(rowcount => 1 + $days_of_data*24*60/$frequency)))
)
Select Time,site_id,sensor_id,row_id,measurement,anomaly_label,name as manufacturer,condition as weather_cond ,voltage,age from CTE1 
left join sensor_manufacturers sm
on CTE1.manufacturer = sm.id
left join weather_cond wc
on CTE1.weather_cond = wc.id;


--- Generating Data for 30 Days for Site -B
Insert into ms_review_for_anomaly_with_add_dimensions
With CTE1
as (
Select 
dateadd(minute,(row_number() over(order by seq) - 1), '2023-01-01 00:00:00') time,
'Site-B' as site_id,
'1' as sensor_id,
row_number() over(order by seq) row_id,
$mean_temp + normal(0,1,random(1)) measurement,
False as anomaly_label,
(uniform(1, 12, random())) manufacturer,
(uniform(1, 8, random())) weather_cond,
(uniform(1.5::float, 3::float, random())) Voltage,
(uniform(1, 5, random())) Age,
from
(Select seq4() as seq from table (generator(rowcount => 1 + $days_of_data*24*60/$frequency)))
)
Select Time,site_id,sensor_id,row_id,measurement,anomaly_label,name as manufacturer,condition as weather_cond ,voltage,age from CTE1 
left join sensor_manufacturers sm
on CTE1.manufacturer = sm.id
left join weather_cond wc
on CTE1.weather_cond = wc.id;



--- INGEST ANOMALY FOR TRANING

--- Update Anomaly Set 1

Update ms_review_for_anomaly_with_add_dimensions
set
measurement =  cte1.measurement,
voltage = cte1.voltage,
manufacturer =cte1.manufacturer,
weather_condition = cte1.weather_cond,
age = cte1.age,
anomaly_label=TRUE
from (
    With CTE1
    as (
        Select
        dateadd(minute,(row_number() over(order by seq) - 1), '2023-01-02 06:00:00') time,
        100 + normal(2,5,random(5)) measurement,
        (2+ uniform(3.0::float, 5::float, random())) voltage,
        (uniform(1,2, random())) manufacturer,
        (uniform(1,2, random())) weather_cond,
        (uniform(5,7, random())) Age
        from
        (Select seq4() as seq from table (generator(rowcount => 150)))
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
where ts between '2023-01-02 06:00:00' and '2023-01-02 08:00:00' and site_id = 'Site-A'
and sensor_id = 1;


--- Update Anomaly Set 2

Update ms_review_for_anomaly_with_add_dimensions
set
measurement =  cte1.measurement,
voltage = cte1.voltage,
weather_condition = cte1.weather_cond,
age = cte1.age,
anomaly_label=TRUE
from (
    With CTE1
    as (
        Select
        dateadd(minute,(row_number() over(order by seq) - 1), '2023-01-05 17:00:00') time,
        100 + normal(2,5,random(5)) measurement,
        (2+ uniform(3.0::float, 5::float, random())) voltage,
        (uniform(7,10, random())) manufacturer,
        (uniform(5,8, random())) weather_cond,
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
where ts between '2023-01-05 17:00:00' and '2023-01-05 18:00:00'  and site_id = 'Site-A'
and sensor_id = 2;



--- Update Anomaly Set 3

Update ms_review_for_anomaly_with_add_dimensions
set
measurement =  cte1.measurement,
voltage = cte1.voltage,
manufacturer =cte1.manufacturer,
weather_condition = cte1.weather_cond,
age = cte1.age,
anomaly_label=TRUE
from (
    With CTE1
    as (
        Select
        dateadd(minute,(row_number() over(order by seq) - 1), '2023-01-07 03:00:00') time,
        100 + normal(2,5,random(5)) measurement,
        (3+ uniform(3.0::float, 5::float, random())) voltage,
        (uniform(7,12, random())) manufacturer,
        (uniform(5,8, random())) weather_cond,
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
and site_id = 'Site-B'
and sensor_id = 1;


Select * from ms_review_for_anomaly_with_add_dimensions 
where ts between '2023-01-07 03:00:00' and '2023-01-07 04:00:00' and site_id = 'Site-B'
and sensor_id = 1;


-- Create training dataset view

Create or replace view training_data as
Select 
ts,
[site_id,sensor_id] as item_id,
measurement,
anomaly_label 
from ms_review_for_anomaly_with_add_dimensions
where ts <= '2023-01-09 23:59:00';

--Create the model. There are two options 1. With label 2 without label. Change it based on customer ask
--We will use supervised model. However you may change based on requirements
Create or replace SNOWFLAKE.ML.ANOMALY_DETECTION MS_DETECT_ANOMALY(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW','training_data'),
    SERIES_COLNAME => 'ITEM_ID',
    TIMESTAMP_COLNAME => 'TS',
    TARGET_COLNAME => 'MEASUREMENT',
    LABEL_COLNAME => 'ANOMALY_LABEL'
);


-- Create or replace SNOWFLAKE.ML.ANOMALY_DETECTION MS_DETECT_ANOMALY(
--     INPUT_DATA => SYSTEM$REFERENCE('VIEW','training_data'),
--     SERIES_COLNAME => 'ITEM_ID',
--     TIMESTAMP_COLNAME => 'TS',
--     TARGET_COLNAME => 'MEASUREMENT',
--     LABEL_COLNAME => '' --- unsupervised
-- );

-- show snowflake.ml.anomaly_detection;



