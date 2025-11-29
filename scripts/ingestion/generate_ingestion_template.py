import os
import pandas as pd
from file_detector import detect_file_type
from pattern_detector import extract_column_formats

RAW_DATA_DIR = r"C:\Users\Aaron\shopzada-data-warehouse\data\raw"
STAGING_DIR = r"C:\Users\Aaron\shopzada-data-warehouse\data\staging"
TEMPLATE_PATH = os.path.join(STAGING_DIR, "ingestion_template.csv")

staging_tables = {}
column_formats = {}

def ingest_folder(folder_path):
    for root, dirs, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                file_type = detect_file_type(file_path)
            except Exception:
                continue

            df = None
            ext = file_type
            if ext == 'csv':
                import readers.csv_reader as reader
                df = reader.read_csv(file_path)
            elif ext in ['xlsx', 'xls']:
                import readers.excel_reader as reader
                df = reader.read_xlsx(file_path)
            elif ext == 'json':
                import readers.json_reader as reader
                df = reader.read_json(file_path)
            elif ext in ['pkl', 'pickle']:
                import readers.pickle_reader as reader
                df = reader.read_pickle(file_path)
            elif ext == 'html':
                import readers.html_reader as reader
                df = reader.read_html(file_path)
            elif ext == 'parquet':
                import readers.parquet_reader as reader
                df = reader.read_parquet(file_path)
            else:
                continue

            if 'Unnamed: 0' in df.columns:
                df = df.drop(columns=['Unnamed: 0'])

            table_name = os.path.basename(root).lower() + "_" + os.path.splitext(file)[0].lower()
            staging_tables[table_name] = df
            column_formats[table_name] = extract_column_formats(df)

            for col, detected in column_formats[table_name].items():
                if detected == ['UNKNOWN']:
                    column_formats[table_name][col] = ['STRING']

def ingest_all():
    for dept in os.listdir(RAW_DATA_DIR):
        dept_path = os.path.join(RAW_DATA_DIR, dept)
        if os.path.isdir(dept_path):
            ingest_folder(dept_path)

def generate_template():
    rows = []
    for table_name, df in staging_tables.items():
        table_columns = df.columns.tolist()
        n_rows = len(df)
        n_cols = len(table_columns)
        sample_values = df.head(1).to_dict(orient='records')[0] if n_rows > 0 else {}
        formats = {col: column_formats[table_name].get(col, ['STRING']) for col in table_columns}

        source_file = "UNKNOWN"
        file_type = "UNKNOWN"
        for root, dirs, files in os.walk(RAW_DATA_DIR):
            for file in files:
                t_name = os.path.basename(root).lower() + "_" + os.path.splitext(file)[0].lower()
                if t_name == table_name:
                    source_file = file
                    try:
                        file_type = detect_file_type(os.path.join(root, file))
                    except:
                        file_type = "UNKNOWN"

        staged_path = os.path.join(STAGING_DIR, f"{table_name}.parquet")

        rows.append({
            "Table Name": table_name,
            "Source File": source_file,
            "File Type": file_type,
            "Rows": n_rows,
            "Columns": n_cols,
            "Column Names": ", ".join(table_columns),
            "Sample Values": sample_values,
            "Column Formats": formats,
            "Staging Path": staged_path
        })

    df_template = pd.DataFrame(rows)
    os.makedirs(STAGING_DIR, exist_ok=True)
    df_template.to_csv(TEMPLATE_PATH, index=False)
    print(f"Ingestion template generated: {TEMPLATE_PATH}")

if __name__ == "__main__":
    ingest_all()
    generate_template()
