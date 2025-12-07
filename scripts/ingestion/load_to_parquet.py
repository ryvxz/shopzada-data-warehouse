import os
import logging
from dotenv import load_dotenv, find_dotenv
from typing import Dict
from scripts.ingestion.data_ingestion.load_raw_data import ingest_folder
from scripts.ingestion.data_quality.quality_checks import check_and_report_quality
from scripts.ingestion.data_quality.generate_report import generate_quality_report

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

def perform_quality_checks(staging_tables: Dict[str, any]) -> Dict[str, any]:
    """Check and report data quality issues."""
    try:
        logger.info("Running data quality checks.")
        staging_tables = check_and_report_quality(staging_tables)
        logger.info("Data quality checks completed successfully.")
    except Exception as e:
        logger.error(f"Error during data quality checks: {e}")
        raise
    return staging_tables

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

def generate_quality_report_and_save(staging_tables: Dict[str, any]):
    """Generate the data quality report."""
    try:
        generate_quality_report(staging_tables, validation_results={})
        logger.info("Data quality report generated successfully.")
    except Exception as e:
        logger.error(f"Error while generating quality report: {e}")
        raise

def main():
    """Main function to run the data pipeline."""
    try:
        check_env_variables()

        # Step 1: Ingest data
        staging_tables = ingest_data()

        # Step 2: Perform data quality checks
        staging_tables = perform_quality_checks(staging_tables)

        # Step 3: Generate quality report
        generate_quality_report_and_save(staging_tables)

        # Step 4: Stage the data as Parquet files
        stage_data(staging_tables)
    
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
