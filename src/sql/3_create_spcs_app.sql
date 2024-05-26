
--- PART 1

use role spcs_app_role;
use warehouse app_wh;
Use database detect_anomaly;
Use schema public;



CREATE SECURITY INTEGRATION IF NOT EXISTS snowservices_ingress_oauth
TYPE=oauth
OAUTH_CLIENT=snowservices_ingress
ENABLED=true;


CREATE OR REPLACE NETWORK RULE ALLOW_ALL_RULE
  TYPE = 'HOST_PORT'
  MODE = 'EGRESS'
  VALUE_LIST= ('0.0.0.0:443', '0.0.0.0:80');


CREATE or REPLACE EXTERNAL ACCESS INTEGRATION ALLOW_ALL_EAI
  ALLOWED_NETWORK_RULES = (ALLOW_ALL_RULE)
  ENABLED = true;

  
create schema anomalyapp;

use schema anomalyapp;

CREATE or REPLACE IMAGE REPOSITORY app_image_repo;


--- PART 2

SHOW IMAGE REPOSITORIES;
SELECT "repository_url" FROM table(result_scan(last_query_id()));

-- List the uploaded image
SELECT SYSTEM$REGISTRY_LIST_IMAGES('/detect_anomaly/anomalyapp/app_image_repo');


CREATE COMPUTE POOL if not exists app_compute_pool
  MIN_NODES = 1
  MAX_NODES = 1
  INSTANCE_FAMILY = CPU_X64_XS;

SHOW COMPUTE POOLS;

DESCRIBE COMPUTE POOL app_compute_pool;

show services;

Drop service if exists streamlit_spcs;


CREATE SERVICE streamlit_spcs
  IN COMPUTE POOL  app_compute_pool
  FROM SPECIFICATION $$
spec:
  containers:
    - name: streamlit
      image: <repository_url>/<app_name>:v1
      env:
        SNOWFLAKE_WAREHOUSE: app_wh
  endpoints:
    - name: streamlit
      port: 8501
      public: true
  $$;



SELECT SYSTEM$GET_SERVICE_STATUS('streamlit_spcs');

SELECT system$get_service_logs('streamlit_spcs', 0, 'streamlit', 500);

SHOW ENDPOINTS IN SERVICE streamlit_spcs;

DESCRIBE SERVICE streamlit_spcs;

