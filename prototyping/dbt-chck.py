'''from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Define DBT project path inside the container
DBT_PROJECT_DIR = "/opt/airflow/dbt/data_pipeline"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='dbt_data_pipeline',
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=['dbt', 'etl'],
) as dag:

    # Task to install dependencies (Ensure DBT is installed)
    install_deps = BashOperator(
        task_id='install_dbt_dependencies',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
    )

    # Task to run DBT models
    run_dbt = BashOperator(
        task_id='run_dbt_models',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir /opt/airflow/dbt/.dbt",
        

    )

    # Task to test DBT models
    # test_dbt = BashOperator(
    #     task_id='test_dbt_models',
    #     bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    # )

    # Define DAG dependencies
    install_deps >> run_dbt #>> test_dbt

'''

from airflow import DAG
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator
from airflow.utils.dates import days_ago

# Define DAG
with DAG(
    "snowflake_dbt_dag",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    # Run dbt models

    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        #dbt_bin="/usr/local/bin/dbt",  # Path to dbt executable
        dir="/opt/airflow/dbt/data_pipeline",
        profiles_dir="/opt/airflow/dbt/.dbt",
    )

    dbt_run