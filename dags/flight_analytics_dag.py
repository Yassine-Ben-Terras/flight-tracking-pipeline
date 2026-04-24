from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'yassine',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG
with DAG(
    dag_id='dbt_flight_analytics_batch',
    default_args=default_args,
    description='Trigger dbt transformations for flight tracking data',
    schedule_interval=None,  # Set to None for manual runs, or use '@daily', '@hourly', etc.
    catchup=False,
    tags=['dbt', 'batch', 'postgres'],
) as dag:

    # dbt_debug uses '|| true' because dbt debug exits with code 1 when git is missing
    # (not installed in the official Airflow image), even though all real checks pass
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/dbt_project/flight_analytics && dbt debug --project-dir /opt/airflow/dbt_project/flight_analytics --profiles-dir /opt/airflow/dbt_project/flight_analytics --log-path /tmp/dbt-logs || true'
    )

    dbt_run_models = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /opt/airflow/dbt_project/flight_analytics && dbt run --project-dir /opt/airflow/dbt_project/flight_analytics --profiles-dir /opt/airflow/dbt_project/flight_analytics --log-path /tmp/dbt-logs'
    )

    dbt_test_data = BashOperator(
        task_id='dbt_test_data',
        bash_command='cd /opt/airflow/dbt_project/flight_analytics && dbt test --project-dir /opt/airflow/dbt_project/flight_analytics --profiles-dir /opt/airflow/dbt_project/flight_analytics --log-path /tmp/dbt-logs'
    )

    # Define the execution pipeline (Dependencies)
    dbt_debug >> dbt_run_models >> dbt_test_data