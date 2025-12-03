import sys
from pathlib import Path
from importlib import import_module

import pendulum

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import chain, DAG, TaskGroup

def run_script(script_folder: str, script_name: str, dag_folder: str):
    """A callable to run scripts from a specified script folder."""
    scripts_path = Path(dag_folder).joinpath('..', '..', 'scripts', script_folder)
    sys.path.insert(0, str(scripts_path))
    module = import_module(script_name)
    module.main() # Assuming each script has a main() function

with DAG(
    dag_id='shopzada_data_warehouse',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse'],
) as dag:
    start = EmptyOperator(task_id='start')

    with TaskGroup('source_staging', tooltip='Source data from dataset and stage it') as source_staging:
        ingest_all_sources = BashOperator(
            task_id='ingest_all_sources',
            bash_command='python ingest.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )

        load_to_staging_db = PythonOperator(
            task_id='load_to_staging_db',
            python_callable=run_script,
            op_kwargs={'script_folder': 'ingestion', 'script_name': 'ingest_to_sql', 'dag_folder': '{{ dag_folder }}'},
        )
        ingest_all_sources >> load_to_staging_db

    with TaskGroup('transform_and_quality_checks', tooltip='Transform data and perform quality checks') as transform_and_quality_checks:
        transform_data = PythonOperator(
            task_id='transform_data',
            python_callable=run_script,
            op_kwargs={'script_folder': 'transformation', 'script_name': 'transform_data', 'dag_folder': '{{ dag_folder }}'},
        )
        quality_checks = PythonOperator(
            task_id='quality_checks',
            python_callable=run_script,
            op_kwargs={'script_folder': 'transformation', 'script_name': 'quality_checks', 'dag_folder': '{{ dag_folder }}'},
        )
        #default dependency transform >> quality

    with TaskGroup('load_to_dw', tooltip='Load data into the data warehouse') as load_to_dw:
        load_physical_model = EmptyOperator(task_id='load_physical_model')

    with TaskGroup('kimball_dw_bigquery_or_postgres', tooltip='Kimball dimensional model') as kimball_dw:
        build_dimensions = EmptyOperator(task_id='build_dimensions')
        build_facts = EmptyOperator(task_id='build_facts')

    with TaskGroup('datamarts_and_views', tooltip='(Optional) Create datamarts and views') as datamarts_and_views:
        create_datamarts = EmptyOperator(task_id='create_datamarts')
        create_views = EmptyOperator(task_id='create_views')

    with TaskGroup('analytics_tableau', tooltip='Run analytics queries') as analytics:
        run_analytics = EmptyOperator(task_id='run_analytics')

    with TaskGroup('presentation_tableau', tooltip='Load data for presentation layer') as presentation:
        load_to_presentation = EmptyOperator(task_id='load_to_presentation')

    end = EmptyOperator(task_id='end')

    chain(
        start,
        source_staging,
        transform_and_quality_checks,
        load_to_dw,
        kimball_dw,
        datamarts_and_views,
        analytics,
        presentation,
        end,
    )
