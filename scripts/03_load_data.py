"""
Python script to load cleaned/transformed data into PostgreSQL database
"""

# importing packages
import os
import csv
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError

# function to load data to PostgreSQL


def load_csv_to_postgres():
    # connect to PostgreSQL database
    try:
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="postgres",
            host="localhost",
            port="5432"
        )
        conn.autocommit = True
        cursor = conn.cursor()

        # specify the directory to search for CSV files
        directory_to_search = '/home/ubuntu/ds/Airflow-ETL-pipeline/data/raw/'

        # find CSV files in the directory
        for filename in os.listdir(directory_to_search):
            if filename.endswith('.csv'):
                file_path = os.path.join(directory_to_search, filename)

                try:
                    # open CSV file and read data
                    with open(file_path, 'r') as file:
                        reader = csv.reader(file)
                        headers = next(reader)

                        # create a SQL command for inserting data
                        insert_query = sql.SQL(
                            'INSERT INTO storedata ({}) VALUES ({})'
                        ).format(
                            sql.SQL(', ').join(map(sql.Identifier, headers)),
                            sql.SQL(', ').join(
                                sql.Placeholder() * len(headers))
                        )

                        # iterate through the rows in the CSV and execute the insert command
                        for row in reader:
                            cursor.execute(insert_query, row)

                    print(f'Appended data from {file_path} to storedata table')

                    # delete the CSV file after successful upload
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
