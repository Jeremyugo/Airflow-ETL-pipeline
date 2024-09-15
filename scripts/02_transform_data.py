"""
Python script to perform minor transformations on a temporarily locally stored CSV file.
This script reads a local CSV, checks for new data based on existing records in a PostgreSQL database,
and updates the CSV with additional columns and transformations.
"""
import logging
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import os
from dotenv import load_dotenv
import sys
# Adds the parent directory to the system path for module imports
sys.path.append('..')

# Set up logging to provide informative messages during script execution
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables from the .env file
load_dotenv()

# Retrieve PostgreSQL connection string from environment variable
postgres_conn = "postgres+psycopg2://postgres:postgres@postgres:5432/postgres"

# Check if the connection string is set properly; exit if missing
if postgres_conn is None:
    logging.error(
        "Environment variable 'AIRFLOW__CORE__SQL_ALCHEMY_CONN' is not set.")
    sys.exit(1)  # Exit with an error code if no connection string

# Modify the connection string to work with psycopg2, which uses 'postgresql' instead of 'postgres+psycopg2'
postgres_conn = postgres_conn.replace('postgres+psycopg2', 'postgresql')


def transform(postgres_conn=postgres_conn):
    """
    Perform data transformation on a locally stored CSV file and update it with new data.

    This function reads a CSV file from the local file system, compares its records with existing data 
    from the PostgreSQL database, computes new columns (e.g., 'Delivery Duration'), removes unnecessary 
    columns, and then saves the updated CSV.

    Args:
        postgres_conn (str): PostgreSQL connection string.
    """
    # Create a connection engine to the PostgreSQL database
    engine = create_engine(postgres_conn)

    # Specify the path to the local CSV file
    local_file_path = "/opt/airflow/data/raw/storedata.csv"

    # Check if the file exists
    if os.path.isfile(local_file_path):
        logging.info(f"Reading file: {local_file_path}")

        try:
            # Load the CSV file into a pandas DataFrame
            df_local = pd.read_csv(local_file_path)
            logging.info(f"Loaded {len(df_local)} rows from local file")

            try:
                # Query the 'storedata' table in the PostgreSQL database
                query = 'SELECT * FROM storedata'
                df = pd.read_sql(query, engine)

                # Find new records by comparing 'Row ID' values in the CSV with those in the database
                idx_ = list(
                    set(df_local['Row ID'].values.tolist()) - set(df['Row ID'].values.tolist()))
            except Exception as e:
                # Log an error if querying the database fails, and assume all rows are new
                logging.error("Error querying PostgreSQL: %s", e)
                idx_ = df_local['Row ID'].values.tolist()

            # Filter the local DataFrame for new records not already in the database
            df_local = df_local[np.isin(df_local['Row ID'], idx_)]

            # Calculate 'Delivery Duration' as the difference between 'Ship Date' and 'Order Date'
            df_local.insert(4, "Delivery Duration", pd.to_datetime(
                df_local['Ship Date']) - pd.to_datetime(df_local['Order Date']))

            # Remove unnecessary columns: 'Country/Region' and 'Customer Name'
            df_local.drop(["Country/Region", "Customer Name"],
                          axis=1, inplace=True)

            # Save the transformed DataFrame back to the CSV file
            df_local.to_csv(local_file_path, index=False)
            logging.info("Data transformed and saved successfully!")
        except Exception as e:
            # Log any errors that occur during file processing
            logging.error("Error processing the file: %s", e)
    else:
        # Log an error if the file is not found
        logging.error(f"File not found: {local_file_path}")


# Main script entry point
if __name__ == "__main__":
    transform()  # Call the transform function
