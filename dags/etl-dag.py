"""
Airflow ETL DAG to automate the ingestion, transformation, and loading of data into
a PostgreSQL database.
"""

# importing necessary packages for the DAG and operators
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
sys.path.append("..")

# Default arguments that apply to all tasks in the DAG
default_args = {
    'owner': 'jerry',  # Owner of the DAG
    'depends_on_past': False,  # DAG execution does not depend on previous runs
    'email_on_failure': False,  # No email alert on failure
    'email_on_retry': False,  # No email alert on retry
    "retry_delay": timedelta(minutes=2)  # Delay between retries
}

# SQL statement for creating the target table in PostgreSQL
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


def transform_data(**Kwargs):
    """
    Function to run a Python script for data transformation.

    Uses a subprocess to execute the transformation script and captures the output and errors.
    """
    import subprocess
    subprocess.run(
        ["python", "/opt/airflow/scripts/02_transform_data.py"],
    )


def load_data(**Kwargs):
    """
    Function to run a Python script for loading data into PostgreSQL.

    Executes the data loading script using a subprocess.
    """
    import subprocess
    subprocess.run(
        ["python", "/opt/airflow/scripts/03_load_data.py"], check=True
    )


# Define the ETL pipeline DAG
with DAG(
    'etl_pipeline_dag',
    default_args=default_args,  # Default arguments applied to all tasks
    description="ETL pipeline to extract CSV files from S3 object storage using Bash, "
                "transform the data, and load it into a PostgreSQL database using Python scripts",
    schedule_interval=None,  # DAG will be triggered manually for now
    start_date=days_ago(1),  # DAG start date (1 day ago)
    catchup=False  # Don't backfill past runs
) as dag:

    # BashOperator to extract data from S3
    extract_task = BashOperator(
        task_id="extract_task",
        # Shell script to extract data
        bash_command="bash /opt/airflow/scripts/01_extract_data.sh ",
    )

    # PostgresOperator to create a table in PostgreSQL
    create_table_task = PostgresOperator(
        task_id='create_storedata_table',
        # PostgreSQL connection ID defined in Airflow
        postgres_conn_id='my_postgres_conn',
        sql=create_table_sql,  # SQL command to create the table
        dag=dag,
    )

    # PythonOperator to run the data transformation script
    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data  # Call the transform_data function
    )

    # PythonOperator to run the data loading script
    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data  # Call the load_data function
    )

    # Define task dependencies: extract -> create table -> transform -> load
    extract_task >> create_table_task >> transform_task >> load_task
