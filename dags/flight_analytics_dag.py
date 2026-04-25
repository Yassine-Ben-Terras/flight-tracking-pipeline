from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'yassine',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

DBT_PROJECT_DIR = '/opt/airflow/dbt_project/flight_analytics'
DBT_PROFILES_DIR = '/opt/airflow/dbt_project/flight_analytics'
DBT_LOGS_DIR = '/tmp/dbt-logs'

with DAG(
    dag_id='dbt_flight_analytics_batch',
    default_args=default_args,
    description='Trigger dbt transformations for flight tracking data',
    # FIX: runs every 15 minutes automatically - no need to trigger manually
    schedule_interval='*/15 * * * *',
    catchup=False,
    tags=['dbt', 'batch', 'postgres'],
) as dag:

    # dbt debug: '|| true' is intentional — dbt debug exits with code 1 when git
    # is missing in the Airflow image, even when all real checks pass.
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command=(
            f'dbt debug '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--log-path {DBT_LOGS_DIR} || true'
        ),
    )

    dbt_run_models = BashOperator(
        task_id='dbt_run_models',
        bash_command=(
            f'dbt run '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--log-path {DBT_LOGS_DIR}'
        ),
    )

    dbt_test_data = BashOperator(
        task_id='dbt_test_data',
        bash_command=(
            f'dbt test '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROFILES_DIR} '
            f'--log-path {DBT_LOGS_DIR}'
        ),
    )

    dbt_debug >> dbt_run_models >> dbt_test_data
