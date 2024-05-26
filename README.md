
# _Environment Setup_
 This is prerequisite to the database setup using the app

## _1. Assumptions_

```
[1]. Conda is already installed on the local machine.
[2]. Docker desptop is already installed on the local machine.
```

## _2. Python virtual environment setup_

```sh

CONDA_SUBDIR=osx-64 conda create -n detectanomaly python=3.8 numpy pandas --override-channels -c https://repo.anaconda.com/pkgs/snowflake -y
conda activate detectanomaly 
conda config --env --set subdir osx-64
pip3 install -r requirements.txt

```
## _3. Snowflake Connectivity Setup_

If you are trying to test the streamlit app locally then you need to add credentials as environmental variables.

```
export SNOWFLAKE_ACCOUNT='<>';
export SNOWFLAKE_ROLE='<>';
export SNOWFLAKE_USER='<>';
export SNOWFLAKE_PASSWORD='<>';
export SNOWFLAKE_DATABASE='<>';
export SNOWFLAKE_SCHEMA='<>';
export SNOWFLAKE_WAREHOUSE='<>';
```


## _4. Data Preparation Script_

#### Prepare the training data and create ml model

1. Execute /src/sql/1_data_gen_n_model_training.sql

#### Prepare the data for inferenceing and create SP for model inference

2. Execute /src/sql/2_data_gen_for_inference.sql

#### Run the model inference for N number of times to detect anomaly on the prepared data

3. Execute /src/python/3_execute_inference.py


## _5. Local testing of your streamlit app_

If the docker container is running hop on to the url http://localhost:8051

If you are testing the app locally then

```sh
streamlit run src/streamlit/app-home.py

```

## _6. Create a docker container and test it locally_

####  Build a docker image from project root and create a container using docker compose

```
docker build --rm --platform linux/amd64 -t <app_name>  -f src/docker/Dockerfile  .
cd src/docker
docker-compose --env-file .env  up 

```

Check the app in action localhost:8051

## _7. Lets try to host the image on SPCS_

#### 1. Execute the sql script from Line 1 to 30 (PART 1)

Execute /src/sql/3_create_spcs_app.sql

#### 2. Get the repository URL

```
SHOW IMAGE REPOSITORIES;
SELECT "repository_url" FROM table(result_scan(last_query_id()));
```

#### 2. Tag the image that you just built

```docker tag <app_name> <repository_url>/<app_name>:v1```


#### 3. Docker Login
#### [snowflake_registry_hostname = organization-account.registry.snowflakecomputing.com]
#### [snowflake_registry_hostname is the first part of your repository url]

```docker login <snowflake_registry_hostname> -u <user_name>```

#### 4. Push the local docker image to snowflake private repository

```docker push <repository_url>/<app_name>:v1```

#### 5. Now its time to execute line 31 till the end (PART 2) of the sql script 

Execute /src/sql/3_create_spcs_app.sql

## _7. Visualise detected anomalies on the app running on SPCS_

Use the URL which is an outout of the following sql command

```SHOW ENDPOINTS IN SERVICE streamlit_spcs;```

## _8. Environment Cleanup_

Execute /src/sql/4_environment_cleanup.sql
