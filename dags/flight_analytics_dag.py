from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default configuration for the pipeline
default_args = {
    'owner': 'yassine',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# DAG Definition
with DAG(
    'dbt_flight_analytics_batch',
    default_args=default_args,
    description='Batch orchestration for dbt on PostgreSQL',
    schedule_interval=timedelta(minutes=15), # Runs every 15 minutes
    start_date=datetime(2024, 4, 1),
    catchup=False,
    tags=['dbt', 'analytics', 'postgresql'],
) as dag:

    # Task 1: Verify dbt installation and connection
    dbt_debug = BashOperator(
        task_id='dbt_debug',
        bash_command='cd /opt/airflow/dbt_project && dbt debug',
    )

    # Task 2: Execute dbt models to update data warehouse tables
    dbt_run = BashOperator(
        task_id='dbt_run_models',
        bash_command='cd /opt/airflow/dbt_project && dbt run',
    )

    # Task 3: Run data quality tests
    dbt_test = BashOperator(
        task_id='dbt_test_data',
        bash_command='cd /opt/airflow/dbt_project && dbt test',
    )

    # Set task dependencies (Execution order)
    dbt_debug >> dbt_run >> dbt_test