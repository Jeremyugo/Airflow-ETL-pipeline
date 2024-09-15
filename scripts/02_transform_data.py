"""
Python script to perform minor transformations on temporarily locally stored csv file
"""
# importing packages
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import sys
sys.path.append('.')

# function to perform python transformation


def transform(user="postgres", password="postgres", database="postgres", host="localhost", port=5432):
    """_summary_

    Args:
        user (str, optional): _description_. Defaults to "postgres".
        password (str, optional): _description_. Defaults to "postgres".
        database (str, optional): _description_. Defaults to "postgres".
        host (str, optional): _description_. Defaults to "localhost".
        port (int, optional): _description_. Defaults to 5432.
    """
    # postgres connection string with default values
    connection_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(connection_string)
    local_file_path = "/home/ubuntu/ds/Airflow-ETL-pipeline/data/raw/storedata.csv"

    # load local file
    df_local = pd.read_csv(local_file_path)

    # try-except block to handle missing local data error
    try:
        query = 'SELECT * FROM storedata'
        df = pd.read_sql(query, engine)
        idx_ = list(set(df_local['Row ID'].values.tolist()
                        ) - set(df['Row ID'].values.tolist()))
    except Exception as e:
        idx_ = df_local['Row ID'].values.tolist()

    # get new subset of data
    df_local = df_local[np.isin(df_local['Row ID'], idx_)]

    df_local.insert(4, "Delivery Duration", pd.to_datetime(
        df_local['Ship Date']) - pd.to_datetime(df_local['Order Date']))
    df_local.drop(["Country/Region", "Customer Name"], axis=1, inplace=True)

    df_local.to_csv(local_file_path, index=False)

    print("Data Transformed!")


if __name__ == "__main__":
    transform()
