"""
Airflow ETL DAG to automate the ingestion, transformation, and loading of data into
a PostgreSQL database.
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import sys
sys.path.append("..")


default_args = {
    'owner': 'jerry',  
    'depends_on_past': False,  
    'email_on_failure': False,  
    'email_on_retry': False, 
    "retry_delay": timedelta(minutes=2) 
}

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
    import subprocess
    subprocess.run(
        ["python", "/opt/airflow/scripts/02_transform_data.py"],
    )


def load_data(**Kwargs):
    import subprocess
    subprocess.run(
        ["python", "/opt/airflow/scripts/03_load_data.py"], check=True
    )


with DAG(
    'etl_pipeline_dag',
    default_args=default_args, 
    description="ETL pipeline to extract CSV files from S3 object storage using Bash, "
                "transform the data, and load it into a PostgreSQL database using Python scripts",
    schedule_interval=None,
    start_date=days_ago(1), 
    catchup=False  
) as dag:


    extract_task = BashOperator(
        task_id="extract_task",
        bash_command="bash /opt/airflow/scripts/01_extract_data.sh ",
    )


    create_table_task = PostgresOperator(
        task_id='create_storedata_table',
        postgres_conn_id='my_postgres_conn',
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


    extract_task >> create_table_task >> transform_task >> load_task
