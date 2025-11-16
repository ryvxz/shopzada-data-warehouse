import glob
import pandas as pd

def ingest_excel():
    excel_files = glob.glob("../../data/raw/**/*.xlsx", recursive=True) + \
                  glob.glob("../../data/raw/**/*.xls", recursive=True)
    csv_files = glob.glob("../../data/raw/**/*.csv", recursive=True)

    all_files = excel_files + csv_files
    print(f"Found Excel/CSV files: {all_files}")

    for file in all_files:
        try:
            if file.lower().endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file)
            elif file.lower().endswith('.csv'):
                df = pd.read_csv(file)
            print(f"[SUCCESS] Loaded file: {file}, rows={len(df)}")
        except Exception as e:
            print(f"[ERROR] Could not load {file}: {e}")