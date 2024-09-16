# Airflow-ETL-pipeline

Containerized ETL pipe line using Apache Airflow and Docker.

#### Workflow

![alt text](./images/etl-1.png)

1. New or Updated CSV is placed in an S3 object storage
2. CSV file is ingested to a temporary local directory using a bash script
3. data is cleaned, transformed and stored in same local directory using a python script
4. transformed data is added/appended to a table in PostgreSQL

**Airflow DAG**
![alt text](./images/image.png)

## Steps to replicate

1. git clone repo

2. create necessary directories and files

```
# cd into project repo
mkdir airflow-logs/scheduler/

# environment files
touch .env
```

**.env file contains**

```
AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS=False
AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgres+psycopg2://postgres:postgres@postgres:5432/postgres
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgres+psycopg2://postgres:postgres@postgres:5432/postgres
AIRFLOW__CORE__FERNET_KEY=81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs=
AIRFLOW_CONN_METADATA_DB=postgres+psycopg2://postgres:postgres@postgres:5432/postgres
AIRFLOW_VAR__METADATA_DB_SCHEMA=airflow
AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC=5
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__WEBSERVER__SECRET_KEY=d8de884371d6718f9e6a2490d638c783b8365d3fc4fcc3a5 # randomly generated using the secretes library
```

3. build and run docker containers

```
docker compose build

docker compose up -d
```

4. if port `8080` is not created, create one in the ports tab in linux panel

5. log into Airflow webserver and create a new Postgres connection in the 'Admin' dropdown with the default config

```
Connection Id = my_postgres_conn
Connection Type = Postgres
Host = postgres
Database = postgres
Login = postgres
Port = postgres # as specified in the docker-compose.yml postgres service
```

6. log into pgadmin4 using port `5050`
   ![alt text](./images/pgadmin4.png)
