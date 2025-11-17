import pandas as pd
import glob
import re
import os

def detect_pattern(value):
    val_str = str(value)
    if re.match(r'^USER\d+', val_str):
        return "USER<id>"
    elif re.match(r'^(\d+)days$', val_str):
        return "<numofdays>days"
    else:
        return ''

def extract_info(df):
    info = []
    for col in df.columns:
        non_null_vals = df[col].dropna().astype(str)
        sample = non_null_vals.iloc[0] if not non_null_vals.empty else ''
        dtype = str(df[col].dtype)
        pattern = detect_pattern(sample)
        info.append((col, dtype, sample, pattern))
    return info

file_patterns = [
    '../../data/raw/**/*.csv',
    '../../data/raw/**/*.xlsx',
    '../../data/raw/**/*.json',
    '../../data/raw/**/*.parquet',
    '../../data/raw/**/*.pickle',
    '../../data/raw/**/*.html'
]

files = []
for pattern in file_patterns:
    files.extend(glob.glob(pattern, recursive=True))

for file in files:
    try:
        if file.endswith('.csv'):
            df = pd.read_csv(file)
        elif file.endswith('.xlsx'):
            df = pd.read_excel(file)
        elif file.endswith('.json'):
            df = pd.read_json(file)
        elif file.endswith('.parquet'):
            df = pd.read_parquet(file)
        elif file.endswith('.pickle'):
            df = pd.read_pickle(file)
        elif file.endswith('.html'):
            tables = pd.read_html(file)
            if len(tables) > 0:
                df = tables[0]
            else:
                continue  # skip if no tables
        else:
            continue

        print(f"File: {file}")
        print("Columns and patterns:")
        info = extract_info(df)
        for col, dtype, sample, pattern in info:
            if pattern:
                print(f"  - {col}: {dtype}, sample: {sample}, pattern: {pattern}")
            else:
                print(f"  - {col}: {dtype}, sample: {sample}")
        print()
    except Exception as e:
        print(f"[ERROR] Could not process {file}: {e}\n")
