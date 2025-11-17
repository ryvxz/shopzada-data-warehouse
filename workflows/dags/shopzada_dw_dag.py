import pendulum

from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain

with DAG(
    dag_id='shopzada_data_warehouse',
    start_date=pendulum.datetime(2025, 11, 17, tz="UTC"),
    catchup=False,
    schedule=None,
    tags=['shopzada', 'data-warehouse'],
) as dag:
    start = EmptyOperator(task_id='start')

    with TaskGroup('source_staging', tooltip='Source data from dataset and stage it into Postgres') as source_staging:
        ingest_excel = BashOperator(
            task_id='ingest_excel',
            bash_command='python ingest_excel.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )
        ingest_html = BashOperator(
            task_id='ingest_html',
            bash_command='python ingest_html.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )
        ingest_json = BashOperator(
            task_id='ingest_json',
            bash_command='python ingest_json.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )
        ingest_parquet = BashOperator(
            task_id='ingest_parquet',
            bash_command='python ingest_parquet.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )
        ingest_pickle = BashOperator(
            task_id='ingest_pickle',
            bash_command='python ingest_pickle.py',
            cwd='{{ dag_folder }}/../../scripts/ingestion'
        )

    with TaskGroup('transform_and_quality_checks', tooltip='Transform data and perform quality checks') as transform_and_quality_checks:
        transform_data = EmptyOperator(task_id='transform_data')
        quality_checks = EmptyOperator(task_id='quality_checks')
        transform_data >> quality_checks

    with TaskGroup('load_to_dw', tooltip='Load data into the data warehouse') as load_to_dw:
        load_physical_model = EmptyOperator(task_id='load_physical_model')

    with TaskGroup('kimball_dw_bigquery_or_postgres', tooltip='Kimball dimensional model') as kimball_dw:
        build_dimensions = EmptyOperator(task_id='build_dimensions')
        build_facts = EmptyOperator(task_id='build_facts')
        build_dimensions >> build_facts

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
