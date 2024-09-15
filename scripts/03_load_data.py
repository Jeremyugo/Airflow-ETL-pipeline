"""
Python script to load cleaned/transformed data from CSV files into a PostgreSQL database.
This script scans a directory for CSV files, uploads the data into a specified PostgreSQL table, 
and removes the files after successful insertion.
"""

# Import required packages for environment variable handling, file I/O, and PostgreSQL connection
from dotenv import load_dotenv
import os
import csv
import psycopg2
from psycopg2 import sql
from psycopg2 import OperationalError
import sys
# Add the parent directory to the system path for imports
sys.path.append('..')

# Load environment variables from the .env file
load_dotenv()

# PostgreSQL connection string
postgres_conn = "postgres+psycopg2://postgres:postgres@postgres:5432/postgres"

# Modify the connection string to replace 'postgres+psycopg2' with 'postgresql' for psycopg2 compatibility
postgres_conn = postgres_conn.replace('postgres+psycopg2', 'postgresql')


def load_csv_to_postgres():
    """
    Loads data from CSV files in a specific directory into a PostgreSQL database table.

    The function connects to the PostgreSQL database, searches for CSV files in a 
    given directory, inserts the data into the 'storedata' table, and deletes the 
    CSV files after successful insertion.
    """
    # Attempt to connect to the PostgreSQL database
    try:
        conn = psycopg2.connect(postgres_conn)  # Connect to the database
        conn.autocommit = True  # Set autocommit to True so changes are applied automatically
        cursor = conn.cursor()  # Create a cursor object to execute SQL commands

        # Specify the directory where CSV files are stored
        directory_to_search = '/opt/airflow/data/raw/'

        # Loop through files in the directory and find files ending with '.csv'
        for filename in os.listdir(directory_to_search):
            if filename.endswith('.csv'):  # Check if the file is a CSV file
                # Get the full file path
                file_path = os.path.join(directory_to_search, filename)

                try:
                    # Open the CSV file and read its contents
                    with open(file_path, 'r') as file:
                        reader = csv.reader(file)  # Create a CSV reader object
                        headers = next(reader)  # Read the first row as headers

                        # Create a SQL INSERT command using the headers from the CSV file
                        insert_query = sql.SQL(
                            'INSERT INTO storedata ({}) VALUES ({})'
                        ).format(
                            # Insert column names
                            sql.SQL(', ').join(map(sql.Identifier, headers)),
                            # Insert placeholders for values
                            sql.SQL(', ').join(
                                sql.Placeholder() * len(headers))
                        )

                        # Loop through the remaining rows in the CSV file and insert data into the database
                        for row in reader:
                            # Execute the SQL command for each row
                            cursor.execute(insert_query, row)

                    print(f'Appended data from {file_path} to storedata table')

                    # After successful upload, delete the CSV file
                    os.remove(file_path)
                    print(f'Deleted {file_path}')

                except Exception as e:
                    # Log any errors encountered while processing a file
                    print(f'Error processing {file_path}: {e}')

        # Close the database cursor and connection after processing all files
        cursor.close()
        conn.close()
        print('All CSV files have been uploaded to the database and cleaned up.')

    except OperationalError as e:
        # Log any errors encountered during the database connection
        print(f'OperationalError: {e}')


# Main entry point: Run the CSV loading function when the script is executed
if __name__ == "__main__":
    load_csv_to_postgres()  # Call the function to load CSV data into PostgreSQL
