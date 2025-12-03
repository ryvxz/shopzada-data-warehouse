import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='source_staging',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'staging'],
) as dag:
    ingest_all_sources = BashOperator(
        task_id='ingest_all_sources',
        bash_command='python ingest.py',
        cwd='{{ dag_folder }}/../../scripts/ingestion'
    )
    
    load_to_staging_db = BashOperator(
        task_id='load_to_staging_db',
        bash_command='python ingest_to_sql.py',
        cwd='{{ dag_folder }}/../../scripts/ingestion'
    )

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform_quality',
        trigger_dag_id='transform_quality',
    )

    ingest_all_sources >> load_to_staging_db >> trigger_transform
