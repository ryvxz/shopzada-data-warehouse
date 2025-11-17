import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='analytics',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'analytics'],
) as dag:
    with TaskGroup('analytics', tooltip='Run analytics queries') as analytics:
        run_analytics = EmptyOperator(task_id='run_analytics')

    trigger_presentation = TriggerDagRunOperator(
        task_id='trigger_presentation',
        trigger_dag_id='presentation',
    )

    analytics >> trigger_presentation
