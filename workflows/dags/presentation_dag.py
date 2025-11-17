import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id='presentation',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'presentation'],
) as dag:
    with TaskGroup('presentation', tooltip='Load data for presentation layer') as presentation:
        load_to_presentation = EmptyOperator(task_id='load_to_presentation')

    # This is the last DAG in the chain
