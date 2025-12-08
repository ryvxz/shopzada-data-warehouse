import pendulum

from airflow.providers.standard.operators.python import PythonOperator
from airflow import DAG
from dag_utils import run_script, DEFAULT_RETRIES

with DAG(
    dag_id='source_staging',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'staging'],
) as dag:
    ingest_all_sources = PythonOperator(
        task_id='ingest_all_sources',
        python_callable=run_script,
        op_kwargs={'script_folder': 'ingestion', 'script_name': 'load_to_parquet'},
        retries=DEFAULT_RETRIES
    )

    data_quality_checks_and_report = PythonOperator(
        task_id = 'data_quality_checks_and_report',
        python_callable = run_script,
        op_kwargs = {'script_folder':'ingestion','script_name':'data_quality_checks'},
        retries = DEFAULT_RETRIES
    )

    load_to_staging_db = PythonOperator(
        task_id='load_to_staging_db',
        python_callable=run_script,
        op_kwargs={'script_folder': 'ingestion', 'script_name': 'load_to_staging'},
        retries=DEFAULT_RETRIES
    )
    ingest_all_sources >> data_quality_checks_and_report >> load_to_staging_db
