"""
Python script to load cleaned/transformed data from CSV files into a PostgreSQL database.
This script scans a directory for CSV files, uploads the data into a specified PostgreSQL table, 
and removes the files after successful insertion.
"""

from dotenv import load_dotenv
import os
import csv
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError
import sys
sys.path.append('..')

load_dotenv()

postgres_conn = "postgres+psycopg2://postgres:postgres@postgres:5432/postgres"

postgres_conn = postgres_conn.replace('postgres+psycopg2', 'postgresql')


def load_csv_to_postgres():
    """
    Loads data from CSV files in a specific directory into a PostgreSQL database table.

    The function connects to the PostgreSQL database, searches for CSV files in a 
    given directory, inserts the data into the 'storedata' table, and deletes the 
    CSV files after successful insertion.
    """
    try:
        conn = psycopg2.connect(postgres_conn) 
        conn.autocommit = True 
        cursor = conn.cursor()  

        directory_to_search = '/opt/airflow/data/raw/'

        for filename in os.listdir(directory_to_search):
            if filename.endswith('.csv'):  
                file_path = os.path.join(directory_to_search, filename)

                try:
                    with open(file_path, 'r') as file:
                        reader = csv.reader(file) 
                        headers = next(reader) 

                        insert_query = sql.SQL(
                            'INSERT INTO storedata ({}) VALUES ({})'
                        ).format(
                            sql.SQL(', ').join(map(sql.Identifier, headers)),
                            sql.SQL(', ').join(
                                sql.Placeholder() * len(headers))
                        )

                        for row in reader:
                            cursor.execute(insert_query, row)

                    print(f'Appended data from {file_path} to storedata table')

                    os.remove(file_path)
                    print(f'Deleted {file_path}')

                except Exception as e:
                    print(f'Error processing {file_path}: {e}')

        cursor.close()
        conn.close()
        print('All CSV files have been uploaded to the database and cleaned up.')

    except OperationalError as e:
        print(f'OperationalError: {e}')


if __name__ == "__main__":
    load_csv_to_postgres() 
