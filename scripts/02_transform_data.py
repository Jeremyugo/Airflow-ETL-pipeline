"""
Python script to perform minor transformations on temporarily locally stored csv file
"""
import logging
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import sys
sys.path.append('..')

# Set up logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load .env file
load_dotenv()

# Retrieve PostgreSQL connection string from environment variable
postgres_conn = "postgres+psycopg2://postgres:postgres@postgres:5432/postgres"

if postgres_conn is None:
    logging.error(
        "Environment variable 'AIRFLOW__CORE__SQL_ALCHEMY_CONN' is not set.")
    sys.exit(1)  # Exit with an error code

# Modify the connection string to remove 'postgres+psycopg2' part since psycopg2 expects only 'postgresql' protocol
postgres_conn = postgres_conn.replace('postgres+psycopg2', 'postgresql')


def transform(postgres_conn=postgres_conn):
    """Perform data transformation on a locally stored CSV file and update it with new data.

    Args:
        postgres_conn (str): PostgreSQL connection string.
    """
    engine = create_engine(postgres_conn)
    local_file_path = "/opt/airflow/data/raw/storedata.csv"

    if os.path.isfile(local_file_path):
        logging.info(f"Reading file: {local_file_path}")

        try:
            df_local = pd.read_csv(local_file_path)
            logging.info(f"Loaded {len(df_local)} rows from local file")

            try:
                query = 'SELECT * FROM storedata'
                df = pd.read_sql(query, engine)
                idx_ = list(
                    set(df_local['Row ID'].values.tolist()) - set(df['Row ID'].values.tolist()))
            except Exception as e:
                logging.error("Error querying PostgreSQL: %s", e)
                idx_ = df_local['Row ID'].values.tolist()

            df_local = df_local[np.isin(df_local['Row ID'], idx_)]
            df_local.insert(4, "Delivery Duration", pd.to_datetime(
                df_local['Ship Date']) - pd.to_datetime(df_local['Order Date']))
            df_local.drop(["Country/Region", "Customer Name"],
                          axis=1, inplace=True)

            df_local.to_csv(local_file_path, index=False)
            logging.info("Data transformed and saved successfully!")
        except Exception as e:
            logging.error("Error processing the file: %s", e)
    else:
        logging.error(f"File not found: {local_file_path}")


if __name__ == "__main__":
    transform()
