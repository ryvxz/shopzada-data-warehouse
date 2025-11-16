import pandas as pd
import glob

def ingest_parquet():
    pq_files = glob.glob("../../data/raw/**/*.parquet", recursive=True)
    print(f"Found Parquet files: {pq_files}")

    for file in pq_files:
        try:
            df = pd.read_parquet(file)
            print(f"[SUCCESS] Loaded Parquet file: {file}, rows={len(df)}")
        except Exception as e:
            print(f"[ERROR] Could not load {file}: {e}")