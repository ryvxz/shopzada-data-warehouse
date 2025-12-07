import os
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from scripts.ingestion.data_ingestion.readers import csv_reader, excel_reader, json_reader, pickle_reader, html_reader, parquet_reader
from scripts.ingestion.data_ingestion.file_detector import detect_file_type

# ------------- ENVIRONMENT SETUP ---------------
# Load environment variables from a .env file
load_dotenv(find_dotenv())

# Load directory paths from environment variables
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
STAGING_DIR = os.getenv("STAGING_DIR")

# ------------- FILE LOADING FUNCTION ---------------
def load_file(file_path, file_type):
    """
    Load a file based on its type.
    Supports csv, xlsx, json, pickle, html, and parquet formats.
    """
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

# ------------- FOLDER INGESTION FUNCTION ---------------
def ingest_folder(folder_path):
    """
    Traverse through all files in a folder, identify the file type, and load the data into memory.
    The data is stored in a dictionary with table names as keys and dataframes as values.
    """
    dataframes = {}
    
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            try:
                # Detect the file type and load the data
                file_type = detect_file_type(file_path)
                df = load_file(file_path, file_type)
            except Exception:
                print(f"Skipped unsupported file type: {file_path}")
                continue

            # Clean up the dataframe by removing unnecessary columns
            if "Unnamed: 0" in df.columns:
                df = df.drop(columns=["Unnamed: 0"])

            # Create a table name based on the folder and file name
            table_name = os.path.basename(root).lower() + "_" + os.path.splitext(file)[0].lower()

            # Print progress for ingestion (optional)
            print(f"Loaded {file} into simulated table {table_name}")
            dataframes[table_name] = df

    return dataframes
