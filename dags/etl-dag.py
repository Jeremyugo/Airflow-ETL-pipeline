""" Airflow ETL DAG to automate the ingestion, transformation and loading of data into
    a PostgreSQL database
"""
# importing packages
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# dag default arguments
default_args = {
    'owner': 'jerry',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=2)
}


# python functions to be used in PythonOperator
def transform_data(**Kwargs):
    import subprocess
    subprocess.run(
        ["python", "/home/ubuntu/ds/Airflow-ETL-pipeline/scripts/02_transform_data.py"], check=True)


# python functions to be used in PythonOperator
def load_data(**Kwargs):
    import subprocess
    subprocess.run(
        ["python", "/home/ubuntu/ds/Airflow-ETL-pipeline/scripts/03_load_data.py"], check=True)


# define the DAG
with DAG(
    'etl_pipeline_dag',
    default_args=default_args,
    description="ETL pipeline to extract csv files from S3 object storage using Bash, transform and load the data into a PostgreSQL database using Python scripts",
    schedule_interval=None,  # manual execution for now
    start_date=days_ago(1),
    catchup=False
) as dag:

    extract_task = BashOperator(
        task_id="extract_task",
        bash_command="bash /home/ubuntu/ds/Airflow-ETL-pipeline/scripts/01_extract_data.sh ",
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data
    )

    # task dependencies
    extract_task >> transform_task >> load_task
