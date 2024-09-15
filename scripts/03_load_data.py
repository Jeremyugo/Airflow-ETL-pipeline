"""
After executing the data_ingestion_1_download_csv_files.sh file, run this file to transform data from CSV format into tables.
"""
import os
import pandas as pd
from sqlalchemy import create_engine

# Connect to a PostgreSQL database using SQLAlchemy.
engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')

# Specify the directory to search for CSV files
directory_to_search = '/home/ubuntu/ds/ETL/data/raw/'

# Keep track of processed directories to remove them if empty
processed_dirs = set()

print('--------- Starting directory walk ---------')
# Walk through the directory
for root, dirs, files in os.walk(directory_to_search, topdown=False):
    for file in files:
        if file.endswith('.csv'):
            file_path = os.path.join(root, file)
            table_name = os.path.splitext(file)[0]  # Extract the table name from the file name
            try:
                # Attempt to read with default UTF-8 encoding
                dataframe = pd.read_csv(file_path)
            except UnicodeDecodeError:
                # If UTF-8 fails, try reading with an alternative encoding
                dataframe = pd.read_csv(file_path, encoding='latin1')
            
            dataframe.to_sql(table_name, engine, if_exists='append', index=False)
            print(f"Uploaded {file} to {table_name} table.")
            os.remove(file_path)  # Remove the processed file
            print(f"Removed {file_path}.")
            processed_dirs.add(root)  # Mark directory for potential removal
            print('---------')

# Remove any empty directories that were processed
print('--------- Starting removal of empty directories ---------')
for processed_dir in processed_dirs:
    if not os.listdir(processed_dir):  # Check if the directory is empty
        os.rmdir(processed_dir)  # Remove the empty directory
        print(f"Removed empty directory {processed_dir}.")
        print('---------')

print('All CSV files have been uploaded to the database and cleaned up.')
