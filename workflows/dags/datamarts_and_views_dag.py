import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow import DAG

with DAG(
    dag_id='datamarts_and_views',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'datamart'],
) as dag:
    create_datamarts = EmptyOperator(task_id='create_datamarts')
    create_views = EmptyOperator(task_id='create_views')
    create_datamarts >> create_views
