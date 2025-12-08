import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow import DAG

with DAG(
    dag_id='kimball_dw',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'kimball'],
) as dag:
    build_dimensions = EmptyOperator(task_id='build_dimensions')
    build_facts = EmptyOperator(task_id='build_facts')
    build_dimensions >> build_facts
