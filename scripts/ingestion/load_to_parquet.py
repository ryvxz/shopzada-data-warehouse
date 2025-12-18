import os
import logging
from dotenv import load_dotenv, find_dotenv
from typing import Dict
from scripts.ingestion.data_ingestion.load_raw_data import ingest_folder
from scripts.ingestion.data_ingestion.group_raw_data import check_file_domain,merge_files
# ------------- ENVIRONMENT SETUP ---------------
load_dotenv(find_dotenv())  # Load environment variables from a .env file

RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
STAGING_DIR = os.getenv("STAGING_DIR")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_env_variables():
    """Ensure that environment variables are set correctly."""
    if not RAW_DATA_DIR or not STAGING_DIR:
        logger.error("Required environment variables (RAW_DATA_DIR, STAGING_DIR) are not set.")
        raise ValueError("Environment variables are missing.")

def ingest_data() -> Dict[str, any]:
    """Ingest data from the raw data directory into a dictionary of DataFrames."""
    staging_tables = {}
    try:
        for department in os.listdir(RAW_DATA_DIR):
            dept_path = os.path.join(RAW_DATA_DIR, department)
            if os.path.isdir(dept_path):
                logger.info(f"Ingesting data from department: {department}")
                dataframes = ingest_folder(dept_path)
                staging_tables.update(dataframes)
    except Exception as e:
        logger.error(f"Error during data ingestion: {e}")
        raise
    return staging_tables

def group_tables(staging_tables: Dict[str, any]):
    processed_files = set()  # Keep track of the files that have already been processed
    
    # Iterate over a snapshot (list) of the dictionary keys to avoid modifying the dictionary while iterating
    for file_name in list(staging_tables.keys()):
        try:
            df = staging_tables[file_name]  # Try to access the DataFrame for the current file
        except KeyError:
            # If the file doesn't exist anymore (because it was merged and removed), log and continue
            print(f"File '{file_name}' does not exist anymore (likely merged). Skipping.")
            continue  # Skip to the next file
        
        # Get the headers of the DataFrame
        headers = list(df.columns)
        
        # Check the domain for this file
        domain = check_file_domain(headers)
        
        if domain == "Unknown file type or unrecognized headers":
            print(f"Skipping file {file_name}: Unrecognized headers.")
            continue
        
        # Check if we have another file in the same domain
        matching_files = [other_file for other_file in staging_tables if other_file != file_name and other_file not in processed_files]
        
        for other_file in matching_files:
            # Get headers of the other file
            other_headers = list(staging_tables[other_file].columns)
            other_domain = check_file_domain(other_headers)

            # If they belong to the same domain, merge them
            if domain == other_domain:
                try:
                    # Merge the two DataFrames
                    new_file_name = domain
                    merge_files(staging_tables, file_name, other_file, new_file_name)
                    
                    # Mark these files as processed
                    processed_files.add(file_name)
                    processed_files.add(other_file)
                    
                    break  # Stop looking for more files to merge with this one
                except Exception as e:
                    print(f"Error merging {file_name} and {other_file}: {e}")
    
    print("Grouping and merging completed.")



def stage_data(staging_tables: Dict[str, any]):
    """Save data to Parquet files in the staging directory."""
    os.makedirs(STAGING_DIR, exist_ok=True)
    try:
        for table_name, df in staging_tables.items():
            staged_path = os.path.join(STAGING_DIR, f"{table_name}.parquet")
            df.to_parquet(staged_path, index=False)
            logger.info(f"Staged table saved at: {staged_path}")
    except Exception as e:
        logger.error(f"Error while staging data: {e}")
        raise

