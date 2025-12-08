import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow import DAG

with DAG(
    dag_id='presentation',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'presentation'],
) as dag:
    load_to_presentation = EmptyOperator(task_id='load_to_presentation')
