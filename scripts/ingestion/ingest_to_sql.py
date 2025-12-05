import pandas as pd
from pyarrow.parquet import ParquetFile
import pyarrow as pa
from sqlalchemy import create_engine
from time import time
import os

from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())


# --- Configuration ---
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
db = os.getenv("DB_NAME")

# This should be the directory path
FILES_FOR_STAGING_DIR = os.getenv("FILES_FOR_STAGING_DIR")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100000))

# Create engine for the postgresql (moved inside main or kept global if needed across modules)
ENGINE = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

# --- Core Functions ---

# Process all parquet files then store into a dictionary {file_name: DataFrame}
def process_all_parquet_files(directory):
    #Reads all Parquet files in a directory into memory as DataFrames.
    df_for_staging = {}
    
    # Process files
    for file_name in os.listdir(directory):
        if file_name.endswith('.parquet'):
            file_path = os.path.join(directory, file_name)
            df = process_parquet_file(file_path)

            # Put df into the dictionary with the file_name as the key
            if df is not None:
                # IMPORTANT: Storing large DataFrames in memory is generally avoided. 
                # The ingest function below uses an optimized, chunk-based approach instead.
                df_for_staging[file_name] = df
                
    return df_for_staging

# Process parquet file, turn it into a DataFrame (simple read)
def process_parquet_file(file_path):
    #Reads a single Parquet file into a Pandas DataFrame.
    try:
        df = pd.read_parquet(file_path)
        return df
    except Exception as e:
        print(f"Error processing file {file_path}: {e}")
        return None

# --- Ingestion Functions ---

def create_table_schema(file_path, table_name, engine):
    #Creates the table schema in the database if it does not exist.
    
    # Read only the schema using ParquetFile
    try:
        # Use ParquetFile to get the schema without reading all data into memory
        pf = ParquetFile(file_path)
        
        # Read a small batch to get a temporary DataFrame for schema creation
        df_iter = pf.iter_batches(batch_size=1) 
        temp_df = pa.Table.from_batches([next(df_iter)]).to_pandas()

        # Create table structure (if_exists='fail' is crucial here)
        temp_df.head(n=0).to_sql(name=table_name, con=engine, if_exists='fail', index=False)
        print(f"Table '{table_name}' created successfully.")
        
    except Exception as e:
        # Check if the table already exists, which is an expected error
        if "already exists" in str(e):
             print(f"Table '{table_name}' already exists, skipping creation.")
        else:
            print(f"An unexpected error occurred during table creation for {table_name}: {e}")

def ingest_file_in_chunks(file_path, table_name, engine):
    print(f"\n--- Starting ingestion for file: {os.path.basename(file_path)} into table: {table_name} ---")
    
    t_start_total = time()
    rows_inserted = 0
    
    try:
        pf = ParquetFile(file_path)
        # Use iter_batches to read data in chunks directly from the Parquet file
        df_iter = pf.iter_batches(batch_size=BATCH_SIZE)
        
        while True:
            t_start_chunk = time()
            
            # Get the next batch of data
            try:
                batch = next(df_iter)
            except StopIteration:
                break # All chunks have been processed

            # Convert pyarrow Batch to pandas DataFrame
            df = pa.Table.from_batches([batch]).to_pandas()
            
            # Append chunk to the PostgreSQL table
            df.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            
            rows_in_chunk = len(df)
            rows_inserted += rows_in_chunk
            t_end_chunk = time()
            
            print(f'  Inserted chunk of {rows_in_chunk} rows, took {t_end_chunk - t_start_chunk:.3f} seconds.')
            
    except Exception as e:
        print(f"An error occurred during chunked ingestion of {table_name}: {e}")

    t_end_total = time()
    print(f"--- Finished ingestion for {table_name}. Total rows: {rows_inserted}. Total time: {t_end_total - t_start_total:.3f} seconds. ---")


def main():
    """Main function to orchestrate the staging process."""
    
    # Check the database connection
    try:
        with ENGINE.connect() as connection:
            print("Successfully connected to the PostgreSQL staging database.")
    except Exception as e:
        print(f"FATAL: Could not connect to the database. Error: {e}")
        return # Exit if connection fails

    # Process files in the staging directory
    for file_name in os.listdir(FILES_FOR_STAGING_DIR):
        if file_name.endswith('.parquet'):
            
            file_path = os.path.join(FILES_FOR_STAGING_DIR or "/opt/airflow/plugins/data/staging", file_name)
            
            table_name = file_name.replace('.parquet', '').lower()
            
            create_table_schema(file_path, table_name, ENGINE)
            
            ingest_file_in_chunks(file_path, table_name, ENGINE)
            
    print("\nAll files processed and ingested into the staging layer.")


if __name__ == "__main__":
    main()