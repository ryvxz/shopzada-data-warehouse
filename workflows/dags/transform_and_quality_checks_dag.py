import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from dag_utils import run_script, DEFAULT_RETRIES

with DAG(
    dag_id='transform_and_quality_checks',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'transform'],
) as dag:
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=run_script,
        op_kwargs={'script_folder': 'ingestion/transform', 'script_name': 'transform'},
        retries=DEFAULT_RETRIES
    )
    quality_checks = PythonOperator(
        task_id='quality_checks',
        python_callable=run_script,
        op_kwargs={'script_folder': 'ingestion/transform', 'script_name': 'quality_checks'},
        retries=DEFAULT_RETRIES
    )
    clean_preprocessed_files = PythonOperator(
        task_id='clean_preprocessed_files',
        python_callable=run_script,
        op_kwargs={'script_folder': 'ingestion', 'script_name': 'clean_preprocessed_files'},
        retries=DEFAULT_RETRIES
    )
    transform_data >> quality_checks >> clean_preprocessed_files
