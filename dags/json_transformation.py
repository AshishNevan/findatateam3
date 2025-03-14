from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.http.sensors.http import HttpSensor
from datetime import datetime, timedelta


MAX_RETRIES = 3
RETRY_DELAY = 60
SNOWFLAKE_CONN_ID = "snowflake_default"

default_args = {
    "owner": "findata_team",
    "start_date": datetime.now(),
    "retries": MAX_RETRIES,
    "retry_delay": timedelta(minutes=RETRY_DELAY),
}

# Create DAG
with DAG(
    "json_transformation",
    default_args=default_args,
    description="Scrape sec data and load to Snowflake",
    schedule_interval="@daily",
    catchup=False,
    params={"year": 2023, "quarter": 4},
) as dag:

    @task.branch(task_id="branch_task")
    def branch_task():
        context = get_current_context()
        res = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID).get_first(
            """
            SELECT COUNT(*) FROM information_schema.tables
            WHERE TABLE_SCHEMA = '{table_schema}'
            AND TABLE_NAME = '{table_name}';
            """.format(
                table_schema="DBT_SCHEMA",
                table_name=f"JSON_{context['params']['year']}Q{context['params']['quarter']}",
            )
        )
        print(res)
        if res[0] > 0:
            return "skip_processing"
        else:
            return "continue_processing"

    @task(task_id="skip_processing")
    def skip_processing():
        print("Data already processed")

    @task(task_id="continue_processing")
    def continue_processing():
        print("Data not processed")

    download_task = HttpOperator(
        task_id="download_task_id",
        http_conn_id="http_backend_default",
        method="GET",
        endpoint="/json/download",
        data={
            "year": "{{ params.year }}",
            "quarter": "{{ params.quarter }}",
        },
        response_filter=lambda response: response.json()["task_id"],
        do_xcom_push=True,
    )

    check_download_task = HttpSensor(
        task_id="check_download_task",
        http_conn_id="http_backend_default",
        method="GET",
        endpoint="/task",
        request_params={"task_id": "{{ ti.xcom_pull('download_task_id') }}"},
        mode="poke",
        response_check=lambda response: response.json()["status"] == "success",
        poke_interval=10,
        timeout=60 * 5,
    )

    transform_task = HttpOperator(
        task_id="transform_task",
        http_conn_id="http_backend_default",
        method="GET",
        endpoint="/json/transform",
        data={
            "year": "{{ params.year }}",
            "quarter": "{{ params.quarter }}",
        },
        response_filter=lambda response: response.json()["task_id"],
        do_xcom_push=True,
    )

    check_transform_task = HttpSensor(
        task_id="check_transform_task",
        http_conn_id="http_backend_default",
        method="GET",
        endpoint="/task",
        request_params={"task_id": "{{ ti.xcom_pull('transform_task') }}"},
        mode="poke",
        response_check=lambda response: response.json()["status"] == "success",
        poke_interval=60 * 60 * 0.5,
        timeout=60 * 60 * 24,
    )

    load_task = HttpSensor(
        task_id="load_task",
        http_conn_id="http_backend_default",
        method="GET",
        endpoint="/json/load",
        request_params={
            "method": "GET",
            "year": "{{ params.year }}",
            "quarter": "{{ params.quarter }}",
        },
        response_check=lambda response: response.json()["status"] == "success",
        poke_interval=30,
        timeout=60 * 10,
    )

    branch_task = branch_task()
    (
        branch_task
        >> continue_processing()
        >> download_task
        >> check_download_task
        >> transform_task
        >> check_transform_task
        >> load_task
    )

    branch_task >> skip_processing()
