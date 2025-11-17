import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id='transform_quality',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse', 'transform'],
) as dag:
    with TaskGroup('transform_and_quality_checks', tooltip='Transform data and perform quality checks') as transform_and_quality_checks:
        transform_data = EmptyOperator(task_id='transform_data')
        quality_checks = EmptyOperator(task_id='quality_checks')
        transform_data >> quality_checks

    trigger_load_dw = TriggerDagRunOperator(
        task_id='trigger_load_to_dw',
        trigger_dag_id='load_to_dw',
    )

    transform_and_quality_checks >> trigger_load_dw
