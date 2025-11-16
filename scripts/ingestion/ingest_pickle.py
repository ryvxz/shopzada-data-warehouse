import glob
import pandas as pd
import os

def ingest_pickle():
    # Recursively find all .pkl and .pickle files
    pickle_files = glob.glob("../../data/raw/**/*.pkl", recursive=True) + \
                   glob.glob("../../data/raw/**/*.pickle", recursive=True)

    for file in pickle_files:
        try:
            df = pd.read_pickle(file)
            print(f"[SUCCESS] Loaded Pickle file: {file}, rows={len(df)}")
        except Exception as e:
            print(f"[ERROR] Could not load Pickle file {file}: {e}")

if __name__ == "__main__":
    ingest_pickle()