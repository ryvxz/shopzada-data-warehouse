import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='kimball_dw',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'kimball'],
) as dag:
    with TaskGroup('kimball_dw', tooltip='Kimball dimensional model') as kimball_dw:
        build_dimensions = EmptyOperator(task_id='build_dimensions')
        build_facts = EmptyOperator(task_id='build_facts')
        build_dimensions >> build_facts

    trigger_datamarts = TriggerDagRunOperator(
        task_id='trigger_datamarts_views',
        trigger_dag_id='datamarts_views',
    )

    kimball_dw >> trigger_datamarts
