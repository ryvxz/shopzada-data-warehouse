import pandas as pd
import pyarrow as pa
from pyarrow.parquet import ParquetFile
from sqlalchemy import create_engine, event
import os
import io
import csv
from time import time
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

# --- Configuration ---
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_STAGING_HOST")
port = os.getenv("DB_PORT")
db = os.getenv("DB_STAGING_NAME")
FILES_FOR_STAGING_DIR = os.getenv("FILES_FOR_STAGING_DIR")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 100000))

# Create engine with a connection pool large enough for parallel processing
ENGINE = create_engine(
    f'postgresql://{user}:{password}@{host}:{port}/{db}',
    pool_size=10,
    max_overflow=20
)

# --- High-Performance COPY Function ---

def psql_insert_copy(table, conn, keys, data_iter):
    """
    Optimized inserter using PostgreSQL COPY command.
    Bypasses standard INSERT overhead.
    """
    dbapi_conn = conn.connection
    with dbapi_conn.cursor() as cur:
        s_buf = io.StringIO()
        writer = csv.writer(s_buf)
        writer.writerows(data_iter)
        s_buf.seek(0)

        columns = ', '.join('"{}"'.format(k) for k in keys)
        table_name = f'"{table.name}"'
        if table.schema:
            table_name = f'{table.schema}.{table_name}'

        sql = f'COPY {table_name} ({columns}) FROM STDIN WITH CSV'
        cur.copy_expert(sql=sql, file=s_buf)

# --- Core Functions ---

def create_table_schema(file_path, table_name):
    """Creates the table schema using a 1-row sample to minimize memory."""
    try:
        pf = ParquetFile(file_path)
        # Just grab the first batch and take 1 row
        first_batch = next(pf.iter_batches(batch_size=1))
        temp_df = pa.Table.from_batches([first_batch]).to_pandas()

        with ENGINE.begin() as conn:
            temp_df.head(0).to_sql(name=table_name, con=conn, if_exists='fail', index=False)
        print(f"✅ Table '{table_name}' created.")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"ℹ️ Table '{table_name}' already exists.")
        else:
            print(f"❌ Error creating schema for {table_name}: {e}")

def ingest_file(file_name):
    """Worker function to process a single file."""
    file_path = os.path.join(FILES_FOR_STAGING_DIR, file_name)
    table_name = file_name.replace('.parquet', '').lower()
    
    # 1. Ensure Table Exists
    create_table_schema(file_path, table_name)
    
    # 2. Bulk Ingest
    print(f"🚀 Starting: {file_name}")
    t_start = time()
    rows_inserted = 0
    
    try:
        pf = ParquetFile(file_path)
        # Use the generator to stream batches from disk
        for batch in pf.iter_batches(batch_size=BATCH_SIZE):
            df = pa.Table.from_batches([batch]).to_pandas()
            
            # Use the psql_insert_copy method for speed
            with ENGINE.begin() as conn:
                df.to_sql(
                    name=table_name, 
                    con=conn, 
                    if_exists='append', 
                    index=False, 
                    method=psql_insert_copy
                )
            rows_inserted += len(df)
            
        t_end = time()
        print(f"✔️ Finished {table_name}: {rows_inserted} rows in {t_end - t_start:.2f}s")
    except Exception as e:
        print(f"❌ Error ingesting {file_name}: {e}")

# --- Execution ---

def main():
    if not os.path.exists(FILES_FOR_STAGING_DIR):
        print(f"FATAL: Directory {FILES_FOR_STAGING_DIR} not found.")
        return

    # Check Connection
    try:
        with ENGINE.connect() as conn:
            print("Connected to PostgreSQL.")
    except Exception as e:
        print(f"FATAL: Connection failed: {e}")
        return

    # Get list of files
    files = [f for f in os.listdir(FILES_FOR_STAGING_DIR) if f.endswith('.parquet')]
    
    if not files:
        print("No Parquet files found.")
        return

    print(f"Found {len(files)} files. Starting parallel ingestion...")

    # Parallelize file processing (adjust max_workers based on your DB capacity)
    with ThreadPoolExecutor(max_workers=4) as executor:
        executor.map(ingest_file, files)

    print("\nAll files processed successfully.")

if __name__ == "__main__":
    main()