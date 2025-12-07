import os
import logging
from dotenv import load_dotenv, find_dotenv

# Load environment variables
load_dotenv(find_dotenv())

STAGING_DIR = os.getenv("STAGING_DIR")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def clean_parquet_files(directory: str):
    if not directory:
        logger.error("STAGING_DIR environment variable is not set.")
        raise ValueError("STAGING_DIR is not defined.")

    if not os.path.isdir(directory):
        logger.warning(f"Directory not found: {directory}. Skipping cleanup.")
        return

    logger.info(f"Starting cleanup of .parquet files in: {directory}")
    deleted_count = 0
    try:
        for filename in os.listdir(directory):
            if filename.endswith(".parquet"):
                file_path = os.path.join(directory, filename)
                os.remove(file_path)
                logger.info(f"Deleted: {file_path}")
                deleted_count += 1
        logger.info(f"Cleanup complete. Deleted {deleted_count} .parquet files.")
    except Exception as e:
        logger.error(f"Error during cleanup of .parquet files in {directory}: {e}")
        raise

def main():
    try:
        clean_parquet_files(STAGING_DIR)
    except Exception as e:
        logger.error(f"Failed to clean preprocessed files: {e}")
        raise

if __name__ == "__main__":
    main()
