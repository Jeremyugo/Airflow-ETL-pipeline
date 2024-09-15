""" Airflow ETL DAG to automate the ingestion, transformation and loading of data into
    a PostgreSQL database
"""
# importing packages
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
sys.path.append("..")

# dag default arguments
default_args = {
    'owner': 'jerry',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    "retry_delay": timedelta(minutes=2)
}


# Define the SQL statement to create the table
create_table_sql = """
CREATE TABLE IF NOT EXISTS "storedata" (
    "Row ID" VARCHAR(36) NOT NULL,
    "Order ID" VARCHAR(36) NOT NULL,
    "Order Date" DATE NOT NULL,
    "Ship Date" DATE NOT NULL,
    "Delivery Duration" VARCHAR(36) NOT NULL,
    "Ship Mode" VARCHAR(36) NOT NULL,
    "Customer ID" VARCHAR(36) NOT NULL,
    "Segment" VARCHAR(36) NOT NULL,
    "City" VARCHAR(36) NOT NULL,
    "State" VARCHAR(36) NOT NULL,
    "Postal Code" VARCHAR(36),
    "Region" VARCHAR(36) NOT NULL,
    "Product ID" VARCHAR(36) NOT NULL,
    "Category" VARCHAR(36) NOT NULL,
    "Sub-Category" VARCHAR(36) NOT NULL,
    "Product Name" VARCHAR(256) NOT NULL,
    "Sales" REAL NOT NULL,
    "Quantity" INTEGER NOT NULL,
    "Discount" REAL NOT NULL,
    "Profit" REAL NOT NULL
);
"""

# python functions to be used in PythonOperator


def transform_data(**Kwargs):
    import subprocess
    import logging

    try:
        result = subprocess.run(
            ["python", "/opt/airflow/scripts/02_transform_data.py"],
            check=True,
            capture_output=True,  # Capture stdout and stderr
            text=True  # Ensure output is in text format
        )
        logging.info("Subprocess completed successfully.")
        logging.info("stdout: %s", result.stdout)
        logging.info("stderr: %s", result.stderr)
    except subprocess.CalledProcessError as e:
        logging.error("Command failed with exit code %d", e.returncode)
        logging.error("stdout: %s", e.stdout)
        logging.error("stderr: %s", e.stderr)
        raise


# python functions to be used in PythonOperator
def load_data(**Kwargs):
    import subprocess
    subprocess.run(
        ["python", "/opt/airflow/scripts/03_load_data.py"], check=True)


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
        bash_command="bash /opt/airflow/scripts/01_extract_data.sh ",
    )

    # Define the PostgresOperator task
    create_table_task = PostgresOperator(
        task_id='create_storedata_table',
        postgres_conn_id='my_postgres_conn',  # Update with your connection ID
        sql=create_table_sql,
        dag=dag,
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
    extract_task >> create_table_task >> transform_task >> load_task
