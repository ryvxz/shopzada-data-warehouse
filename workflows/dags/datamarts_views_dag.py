import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='datamarts_views',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'datamart'],
) as dag:
    with TaskGroup('datamarts_and_views', tooltip='(Optional) Create datamarts and views') as datamarts_and_views:
        create_datamarts = EmptyOperator(task_id='create_datamarts')
        create_views = EmptyOperator(task_id='create_views')

    trigger_analytics = TriggerDagRunOperator(
        task_id='trigger_analytics',
        trigger_dag_id='analytics',
    )

    datamarts_and_views >> trigger_analytics
