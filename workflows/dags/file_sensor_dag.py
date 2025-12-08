import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import timedelta

from dotenv import load_dotenv
import os
load_dotenv()

data = os.getenv("DATA_FOLDER", '/opt/airflow/plugins/data')

with DAG(
    dag_id='file_sensor_dag',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=timedelta(minutes=2),
    tags=['shopzada', 'data-warehouse', 'sensor'],
    is_paused_upon_creation=False,
    max_active_runs=1,
    
) as dag:
    wait_for_file = FileSensor(
        task_id='wait_for_new_file',
        filepath=f'{data}/new/**/*',
        recursive=True,
        poke_interval=10,
        timeout=60,
        mode='poke',
    )
    move_data = BashOperator(
    task_id='move_data',
    # Use find to list all files and execute mv on them.
    # -mindepth 1 prevents it from matching the base directory itself.
    # The || true ensures the task succeeds even if no files are found (though unlikely after the sensor)
    bash_command=f"find {data}/new/ -mindepth 1 -exec mv -t {data}/raw/ {{}} + || true",
)

    trigger_main_dag = TriggerDagRunOperator(
        task_id='trigger_main_dag',
        trigger_dag_id='shopzada_data_warehouse',
        wait_for_completion=False,
    )

    wait_for_file >> move_data >> trigger_main_dag
