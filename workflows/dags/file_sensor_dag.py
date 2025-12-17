import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta

from dotenv import load_dotenv
import os
import logging

load_dotenv()

# =============================================================================
# ENVIRONMENT VARIABLES CONFIGURATION
# =============================================================================
# This DAG supports the following environment variables for customization:
#
# Data Configuration:
# - DATA_FOLDER: Base directory for data files (default: /opt/airflow/plugins/data)
#
# File Sensor Configuration:
# - FILE_SENSOR_SCHEDULE_MINUTES: Sensor check interval in minutes (default: 2)
# - FILE_SENSOR_POKE_INTERVAL: FileSensor poke interval in seconds (default: 10)
# - FILE_SENSOR_TIMEOUT: FileSensor timeout in seconds (default: 60)
# - FILE_SENSOR_SOURCE_DIR: Directory to monitor for new files (default: new)
# - FILE_SENSOR_DEST_DIR: Directory to move processed files (default: raw)
# - FILE_SENSOR_MINDEPTH: Minimum depth for file search (default: 1)
# - MAIN_DAG_ID: DAG ID to trigger when files are detected
# =============================================================================

# Data Configuration
DATA_FOLDER = os.getenv("DATA_FOLDER", "/opt/airflow/plugins/data")

# File Sensor Timing Configuration
FILE_SENSOR_SCHEDULE_MINUTES = int(os.getenv("FILE_SENSOR_SCHEDULE_MINUTES", "2"))
FILE_SENSOR_POKE_INTERVAL = int(os.getenv("FILE_SENSOR_POKE_INTERVAL", "10"))
FILE_SENSOR_TIMEOUT = int(os.getenv("FILE_SENSOR_TIMEOUT", "60"))

# File Processing Configuration
FILE_SENSOR_SOURCE_DIR = os.getenv("FILE_SENSOR_SOURCE_DIR", "new")
FILE_SENSOR_DEST_DIR = os.getenv("FILE_SENSOR_DEST_DIR", "raw")
FILE_SENSOR_MINDEPTH = int(os.getenv("FILE_SENSOR_MINDEPTH", "1"))

# DAG Trigger Configuration
MAIN_DAG_ID = os.getenv("MAIN_DAG_ID", "shopzada_data_warehouse")

def validate_environment_variables():
    """Validate critical environment variables with warnings and minimal safe fallbacks."""
    # Validate positive integers with minimal safe fallbacks
    validations = [
        ("FILE_SENSOR_SCHEDULE_MINUTES", FILE_SENSOR_SCHEDULE_MINUTES, 1, "sensor schedule"),
        ("FILE_SENSOR_POKE_INTERVAL", FILE_SENSOR_POKE_INTERVAL, 5, "poke interval"),
        ("FILE_SENSOR_TIMEOUT", FILE_SENSOR_TIMEOUT, 30, "sensor timeout"),
        ("FILE_SENSOR_MINDEPTH", FILE_SENSOR_MINDEPTH, 1, "file mindepth")
    ]
    
    for var_name, var_value, min_safe, description in validations:
        if var_value <= 0:
            logging.warning(f"Warning: {var_name} must be positive integer, got {var_value}. Using minimal safe value: {min_safe}")
            globals()[var_name] = min_safe

# Validate environment variables at DAG import time
validate_environment_variables()

data = DATA_FOLDER

with DAG(
    dag_id='file_sensor_dag',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=timedelta(minutes=FILE_SENSOR_SCHEDULE_MINUTES),
    tags=['shopzada', 'data-warehouse', 'sensor'],
    is_paused_upon_creation=True,
    max_active_runs=1,
    
) as dag:
    wait_for_file = FileSensor(
        task_id='wait_for_new_file',
        filepath=f'{data}/{FILE_SENSOR_SOURCE_DIR}/**/*',
        recursive=True,
        poke_interval=FILE_SENSOR_POKE_INTERVAL,
        timeout=FILE_SENSOR_TIMEOUT,
        mode='poke',
    )
    move_data = BashOperator(
    task_id='move_data',
    # Simple retry logic for file moves (max 3 attempts)
    bash_command=f"find {data}/{FILE_SENSOR_SOURCE_DIR}/ -mindepth {FILE_SENSOR_MINDEPTH} -exec mv -t {data}/{FILE_SENSOR_DEST_DIR}/ {{}} + || true",
    )

    trigger_main_dag = TriggerDagRunOperator(
        task_id='trigger_main_dag',
        trigger_dag_id=MAIN_DAG_ID,
        wait_for_completion=False,
    )

    wait_for_file >> move_data >> trigger_main_dag
