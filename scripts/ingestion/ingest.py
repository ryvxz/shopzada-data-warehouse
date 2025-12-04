import os
import pandas as pd
from readers import csv_reader, excel_reader, json_reader, pickle_reader, html_reader, parquet_reader
from pattern_detector import extract_column_formats
from file_detector import detect_file_type

RAW_DATA_DIR = r"/opt/airflow/plugins/data/raw"
STAGING_DIR = r"/opt/airflow/plugins/data/staging"

staging_tables = {}
column_formats = {}

def load_file(file_path, file_type):
    if file_type == "csv":
        return csv_reader.read_csv(file_path)
    elif file_type in ["xlsx", "xls"]:
        return excel_reader.read_xlsx(file_path)
    elif file_type == "json":
        return json_reader.read_json(file_path)
    elif file_type in ["pkl", "pickle"]:
        return pickle_reader.read_pickle(file_path)
    elif file_type == "html":
        return html_reader.read_html(file_path)
    elif file_type == "parquet":
        return parquet_reader.read_parquet(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")

def ingest_folder(folder_path):
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                file_type = detect_file_type(file_path)
                df = load_file(file_path, file_type)
            except Exception:
                print(f"Skipped unsupported file type: {file_path}")
                continue

            if "Unnamed: 0" in df.columns:
                df = df.drop(columns=["Unnamed: 0"])

            table_name = os.path.basename(root).lower() + "_" + os.path.splitext(file)[0].lower()
            staging_tables[table_name] = df

            formats = extract_column_formats(df)
            for col, detected in formats.items():
                if detected == ["UNKNOWN"]:
                    formats[col] = ["STRING"]
            column_formats[table_name] = formats

            print(f"Loaded {file} into simulated table {table_name}")
            print(f"Column formats: {column_formats[table_name]}")

if __name__ == "__main__":
    for department in os.listdir(RAW_DATA_DIR):
        dept_path = os.path.join(RAW_DATA_DIR, department)
        if os.path.isdir(dept_path):
            ingest_folder(dept_path)

    print("\nAll tables loaded. Available staging tables:")
    for table in staging_tables.keys():
        print(table)

    os.makedirs(STAGING_DIR, exist_ok=True)
    for table_name, df in staging_tables.items():
        staged_path = os.path.join(STAGING_DIR, f"{table_name}.parquet")
        df.to_parquet(staged_path, index=False)
        print(f"Staged table saved: {staged_path}")

    rows = [
        {"table_name": table_name, "column_name": col_name, "detected_format": ",".join(detected)}
        for table_name, formats in column_formats.items()
        for col_name, detected in formats.items()
    ]

    df_report = pd.DataFrame(rows)
    report_path = os.path.join(STAGING_DIR, "data_quality_report.csv")
    df_report.to_csv(report_path, index=False)
    print(f"Data quality report saved: {report_path}")