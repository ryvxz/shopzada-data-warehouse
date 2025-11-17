import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='source_staging',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'staging'],
) as dag:
    with TaskGroup('source_staging', tooltip='Source data from dataset and stage it into Postgres') as source_staging:
        ingest_excel = BashOperator(
            task_id='ingest_excel',
            bash_command='python ingest_excel.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )
        ingest_html = BashOperator(
            task_id='ingest_html',
            bash_command='python ingest_html.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )
        ingest_json = BashOperator(
            task_id='ingest_json',
            bash_command='python ingest_json.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )
        ingest_parquet = BashOperator(
            task_id='ingest_parquet',
            bash_command='python ingest_parquet.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )
        ingest_pickle = BashOperator(
            task_id='ingest_pickle',
            bash_command='python ingest_pickle.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )

    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform_quality',
        trigger_dag_id='transform_quality',
    )

    source_staging >> trigger_transform
