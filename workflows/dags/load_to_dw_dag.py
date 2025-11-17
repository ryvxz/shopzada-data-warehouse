import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='load_to_dw',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'load-dw'],
) as dag:
    with TaskGroup('load_to_dw', tooltip='Load data into the data warehouse') as load_to_dw:
        load_physical_model = EmptyOperator(task_id='load_physical_model')

    trigger_kimball_dw = TriggerDagRunOperator(
        task_id='trigger_kimball_dw',
        trigger_dag_id='kimball_dw',
    )

    load_to_dw >> trigger_kimball_dw
