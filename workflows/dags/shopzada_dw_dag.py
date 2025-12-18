import sys
from pathlib import Path
from importlib import import_module

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import chain, DAG, TaskGroup
from airflow.providers.standard.sensors.filesystem import FileSensor


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
# - DEFAULT_RETRIES: Number of retry attempts for failed tasks (default: 3)
#
# Script Configuration:
# - DEFAULT_SCRIPTS_FOLDER: Base directory for Python scripts
# - INGESTION_FOLDER: Subfolder for ingestion scripts (default: ingestion)
# - TRANSFORM_FOLDER: Subfolder for transformation scripts (default: ingestion/transform)
# - [Script name variables]: Override specific script filenames
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
DEFAULT_RETRIES = int(os.getenv("DEFAULT_RETRIES", 3))

# Script Configuration
DEFAULT_SCRIPTS_FOLDER = os.getenv("DEFAULT_SCRIPTS_FOLDER", "/opt/airflow/plugins/scripts")
INGESTION_FOLDER = os.getenv("INGESTION_FOLDER", "ingestion")
INGESTION_PARQUET_FOLDER = os.getenv("INGESTION_FOLDER", "ingestion/to_parquet")
DIMENSION_FOLDER = os.getenv("DIMENSION_FOLDER", "ingestion/transform/dimension")
FACT_FOLDER = os.getenv("FACT_FOLDER","ingestion/transform/fact")
LOADING_FOLDER = os.getenv("LOADING_FOLDER","loading")

# Script Names
DATA_QUALITY_CHECKS_SCRIPT = os.getenv("DATA_QUALITY_CHECKS_SCRIPT", "data_quality_checks")
LOAD_TO_STAGING_SCRIPT = os.getenv("LOAD_TO_STAGING_SCRIPT", "load_to_staging")
TRANSFORM_SCRIPT = os.getenv("TRANSFORM_SCRIPT", "transform_tables")
QUALITY_CHECKS_SCRIPT = os.getenv("QUALITY_CHECKS_SCRIPT", "quality_checks")
CLEAN_PREPROCESSED_FILES_SCRIPT = os.getenv("CLEAN_PREPROCESSED_FILES_SCRIPT", "clean_preprocessed_files")

#

# File Sensor Configuration (for validation)
FILE_SENSOR_SCHEDULE_MINUTES = int(os.getenv("FILE_SENSOR_SCHEDULE_MINUTES", "2"))
FILE_SENSOR_POKE_INTERVAL = int(os.getenv("FILE_SENSOR_POKE_INTERVAL", "10"))
FILE_SENSOR_TIMEOUT = int(os.getenv("FILE_SENSOR_TIMEOUT", "60"))
FILE_SENSOR_MINDEPTH = int(os.getenv("FILE_SENSOR_MINDEPTH", "1"))


def validate_environment_variables():
    """Validate critical environment variables with warnings and minimal safe fallbacks."""
    # Validate positive integers with minimal safe fallbacks
    validations = [
        ("DEFAULT_RETRIES", DEFAULT_RETRIES, 1, "retries"),
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

folder = os.getenv("SCRIPTS_FOLDER")

def run_script(script_folder: str, script_name: str):
    """A callable to run scripts from a specified script folder."""
    scripts_path = Path(folder or DEFAULT_SCRIPTS_FOLDER).joinpath(script_folder)
    sys.path.insert(0, str(scripts_path))
    try:
        module = import_module(script_name)
        module.main()  # Assuming each script has a main() function
    finally:
        sys.path.pop(0)

with DAG(
    dag_id='shopzada_data_warehouse',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse'],
    is_paused_upon_creation=False,
    max_active_runs=1,
) as dag:
    start = EmptyOperator(task_id='start')
    

    with TaskGroup('source_staging', tooltip='Source data from dataset and stage it') as source_staging:

        folders = ['business','customer','enterprise','marketing','operations']
        folder_ingestion_task = []
        for folder in folders:
            task = PythonOperator(
                task_id=f'load_{folder}_to_parquet',
                python_callable=run_script,
                op_kwargs={'script_folder': INGESTION_PARQUET_FOLDER, 'script_name': f"load_{folder}_to_parquet"},
                retries=DEFAULT_RETRIES
            )
            folder_ingestion_task.append(task)

        # data_quality_checks_and_report = PythonOperator(
        #     task_id = 'data_quality_checks_and_report',
        #     python_callable = run_script,
        #     op_kwargs = {'script_folder': INGESTION_FOLDER, 'script_name': DATA_QUALITY_CHECKS_SCRIPT},
        #     retries = DEFAULT_RETRIES
        # )

        load_to_staging_db = PythonOperator(
            task_id='load_to_staging_db',
            python_callable=run_script,
            op_kwargs={'script_folder': INGESTION_FOLDER, 'script_name': LOAD_TO_STAGING_SCRIPT},
            retries=DEFAULT_RETRIES
        )

        clean_preprocessed_files = PythonOperator(
            task_id='clean_preprocessed_files',
            python_callable=run_script,
            op_kwargs={'script_folder': INGESTION_FOLDER, 'script_name': CLEAN_PREPROCESSED_FILES_SCRIPT},
            retries=DEFAULT_RETRIES
        )
        folder_ingestion_task >> load_to_staging_db >> clean_preprocessed_files


    with TaskGroup('transform_and_load_dim', tooltip='Transform and load dimension tables') as transform_and_load_dim:
    
        dims = ['campaign', 'customer', 'date', 'merchant', 'product', 'staff']
        
        create_dimension_tables = PythonOperator(
            task_id='create_dimension_tables',
            python_callable=run_script,
            op_kwargs={'script_folder': LOADING_FOLDER, 'script_name': "create_dimension_tables"},
            retries=DEFAULT_RETRIES
        )

        # Create tasks dynamically
        dim_tasks = []
        for dim in dims:
            task = PythonOperator(
                task_id=f'dim_{dim}',
                python_callable=run_script,
                op_kwargs={'script_folder': DIMENSION_FOLDER, 'script_name': f"dim_{dim}"},
                retries=DEFAULT_RETRIES
            )
            dim_tasks.append(task)

        

        # All tasks in the list run in parallel, then trigger the clean task
        create_dimension_tables >> dim_tasks 

    with TaskGroup('transform_and_load_fact', tooltip='Transform and load fact tables') as transform_and_load_fact:
    
        facts = ['campaign_transaction','order_line_item','order_delay']
        
        create_fact_tables = PythonOperator(
            task_id='create_fact_tables',
            python_callable=run_script,
            op_kwargs={'script_folder': LOADING_FOLDER, 'script_name': "create_fact_tables"},
            retries=DEFAULT_RETRIES
        )

        # Create tasks dynamically
        fact_tasks = []
        for fact in facts:
            task = PythonOperator(
                task_id=f'fact_{fact}',
                python_callable=run_script,
                op_kwargs={'script_folder': FACT_FOLDER, 'script_name': f"fact_{fact}"},
                retries=DEFAULT_RETRIES
            )
            dim_tasks.append(task)

         # All tasks in the list run in parallel, then trigger the clean task
        create_fact_tables >> fact_tasks 

    with TaskGroup('cleanup_temp_tables', tooltip='Cleanup temporary tables') as cleanup_temp_table:
        cleanup_temp_tables = PythonOperator(
            task_id='cleanup_temp_tables',
            python_callable=run_script,
            op_kwargs={'script_folder': LOADING_FOLDER, 'script_name': "cleanup"},
            retries=DEFAULT_RETRIES
        )

        cleanup_temp_tables
        

        


    # with TaskGroup('datamarts_and_views', tooltip='(Optional) Create datamarts and views') as datamarts_and_views:
    #     create_datamarts = EmptyOperator(task_id='create_datamarts')
    #     create_views = EmptyOperator(task_id='create_views')

    # with TaskGroup('analytics_tableau', tooltip='Run analytics queries') as analytics:
    #     run_analytics = EmptyOperator(task_id='run_analytics')

    # with TaskGroup('presentation_tableau', tooltip='Load data for presentation layer') as presentation:
    #     load_to_presentation = EmptyOperator(task_id='load_to_presentation')

    end = EmptyOperator(task_id='end')

    chain(
        start,
        source_staging,
        transform_and_load_dim,
        transform_and_load_fact,
        cleanup_temp_table,
        # datamarts_and_views,
        # analytics,
        # presentation,
        end,
    )