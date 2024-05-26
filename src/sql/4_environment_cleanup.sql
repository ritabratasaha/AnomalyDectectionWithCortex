use role spcs_app_role;
use warehouse app_wh;
Use database detect_anomaly;
Use schema anomalyapp;

Drop Service if exists streamlit_spcs;
  
Drop compute pool if exists app_compute_pool;

Drop database if exists detect_anomaly;

Drop warehouse if exists app_wh;

use role accountadmin;

drop role spcs_app_role;

