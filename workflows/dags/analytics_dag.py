import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow import DAG

with DAG(
    dag_id='analytics',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'analytics'],
) as dag:
    run_analytics = EmptyOperator(task_id='run_analytics')
