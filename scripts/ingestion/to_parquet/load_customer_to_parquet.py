from scripts.ingestion.load_to_parquet import check_env_variables, ingest_data, stage_data, group_tables
from scripts.ingestion.data_ingestion.load_raw_data import ingest_folder
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR")
STAGING_DIR = os.getenv("STAGING_DIR")


def main():
    """Main function to run the data ingestion pipeline."""
    try:
        check_env_variables()
        folder_dir = os.path.join(RAW_DATA_DIR,"customer")
        # Step 1: Ingest data
        staging_tables = ingest_folder(folder_dir)

        # Step 2: Group tables
        group_tables(staging_tables)

        # Step 3: Stage the data as Parquet files
        stage_data(staging_tables)


    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        raise

if __name__ == "__main__":
    main()
