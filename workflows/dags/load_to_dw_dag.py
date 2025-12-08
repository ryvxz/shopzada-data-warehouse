import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow import DAG

with DAG(
    dag_id='load_to_dw',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'load'],
) as dag:
    load_physical_model = EmptyOperator(task_id='load_physical_model')
