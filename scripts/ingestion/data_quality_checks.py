import os
import logging
import pandas as pd
from dotenv import load_dotenv, find_dotenv
from typing import Dict
from scripts.ingestion.data_quality.quality_checks import check_and_report_quality
from scripts.ingestion.data_quality.generate_report import generate_quality_report

# ------------- ENVIRONMENT SETUP ---------------
load_dotenv(find_dotenv())  # Load environment variables from a .env file

STAGING_DIR = os.getenv("STAGING_DIR")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def check_env_variables():
    """Ensure that environment variables are set correctly."""
    if not STAGING_DIR:
        logger.error("Required environment variables (STAGING_DIR) are not set.")
        raise ValueError("Environment variables are missing.")

def load_staged_data() -> Dict[str, pd.DataFrame]:
    """Load all Parquet files from the staging directory into a dictionary."""
    staging_tables = {}
    try:
        for filename in os.listdir(STAGING_DIR):
            if filename.endswith('.parquet'):
                table_name = filename.replace('.parquet', '')
                table_path = os.path.join(STAGING_DIR, filename)
                df = pd.read_parquet(table_path)
                staging_tables[table_name] = df
                logger.info(f"Loaded table: {table_name}")
    except Exception as e:
        logger.error(f"Error loading staged data: {e}")
        raise
    return staging_tables

def perform_quality_checks(staging_tables: Dict[str, pd.DataFrame]) -> Dict[str, any]:
    """Check and report data quality issues."""
    try:
        logger.info("Running data quality checks.")
        staging_tables = check_and_report_quality(staging_tables)
        logger.info("Data quality checks completed successfully.")
    except Exception as e:
        logger.error(f"Error during data quality checks: {e}")
        raise
    return staging_tables

def update_parquet_files(staging_tables: Dict[str, pd.DataFrame]):
    """Overwrite existing Parquet files with updated data after quality checks."""
    try:
        for table_name, df in staging_tables.items():
            file_path = os.path.join(STAGING_DIR, f"{table_name}.parquet")
            # Overwrite the Parquet file with the updated DataFrame
            df.to_parquet(file_path, index=False)
            logger.info(f"Updated Parquet file saved at: {file_path}")
    except Exception as e:
        logger.error(f"Error updating Parquet files: {e}")
        raise

def generate_quality_report_and_save(staging_tables: Dict[str, any]):
    """Generate the data quality report."""
    try:
        generate_quality_report(staging_tables, validation_results={})
        logger.info("Data quality report generated successfully.")
    except Exception as e:
        logger.error(f"Error while generating quality report: {e}")
        raise

def main():
    """Main function to run the data quality check pipeline."""
    try:
        check_env_variables()

        # Step 1: Load staged data (Parquet files)
        staging_tables = load_staged_data()

        # Step 2: Perform data quality checks
        staging_tables = perform_quality_checks(staging_tables)

        # Step 3: Update the Parquet files with the updated data
        update_parquet_files(staging_tables)

        # Step 4: Generate quality report
        generate_quality_report_and_save(staging_tables)
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
