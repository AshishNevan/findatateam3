from datetime import datetime
import os
from pathlib import Path
import shutil
import time
from typing import Dict
import requests
from enum import Enum
import logging
import uuid

from scripts import download_with_retry, load_data
from sec_json import transform_to_json

from fastapi import BackgroundTasks, FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(".env")
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = FastAPI(title="FastAPI Backend", version="0.1.0")

class QueryRequest(BaseModel):
    sql: str

SNOWFLAKE_URL = (
    "snowflake://{user}:{password}@{account}/{db}/{schema}?{wh}={wh}&role={role}"
).format(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    db=os.getenv("SNOWFLAKE_DB"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    wh=os.getenv("SNOWFLAKE_WH"),
    role=os.getenv("SNOWFLAKE_ROLE"),
)


class QueryRequest(BaseModel):
    sql: str


class task(BaseModel):
    name: str
    status: str


class Conf(BaseModel):
    quarter: int
    year: int


class Dags(Enum):
    json_transformation = "json"
    sec_data_pipeline = "normalized"
    snow = "denormalized"


tasks: Dict[uuid.UUID, task] = {}

print(os.getenv("AIRFLOW_PUBLIC_IP"))

AIRFLOW_URL = "http://{airflow_host}/api/v1".format(
    airflow_host=os.getenv("AIRFLOW_PUBLIC_IP", "127.0.0.1:8080")
)

engine = create_engine(SNOWFLAKE_URL)


def download_task(task_id: uuid.UUID, year: int, quarter: int):
    tasks[task_id] = task(name="download", status="running")
    flag = download_with_retry(year=year, quarter=quarter)
    tasks[task_id] = task(name="download", status="success" if flag else "failed")


def transform_task(task_id: uuid.UUID, year: int, quarter: int):
    tasks[task_id] = task(name="transform", status="running")
    flag = transform_to_json(year=year, quarter=quarter)
    tasks[task_id] = task(name="transform", status="success" if flag else "failed")


@app.get("/")
def read_root():
    return {"message": "Welcome to the FastAPI backend!"}


@app.get("/health")
def read_health():
    return {"status": "ok"}


@app.post("/airflow/rundag/")
def run_airflow_dag(dag_id: Dags, conf: Conf):
    """
    Sends a POST request to the Airflow API to trigger a DAG run.
    """
    url = f"{AIRFLOW_URL}/dags/{dag_id.name}/dagRuns"
    print(url)
    initiated_on = datetime.now().isoformat()
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    auth = ("airflow", "airflow")
    response = requests.post(
        url,
        json={
            "conf": {"year": conf.year, "quarter": conf.quarter},
        },
        headers=headers,
        auth=auth,
    )
    return response.json()


@app.get("/airflow/dagrun/")
def get_airflow_dag(dag_id: Dags, dag_run_id: str):
    """
    Get status of a DAG run.
    """
    url = f"{AIRFLOW_URL}/dags/{dag_id.name}/dagRuns/{dag_run_id}"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    auth = ("airflow", "airflow")
    response = requests.get(url, headers=headers, auth=auth)
    return response.json()


@app.get("/airflow/dagruns/")
def get_airflow_dagruns(dag_id: Dags):
    """
    Get all DAG runs for a specific DAG.
    """
    url = f"{AIRFLOW_URL}/dags/{dag_id.name}/dagRuns"
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    auth = ("airflow", "airflow")
    response = requests.get(url, headers=headers, auth=auth)
    return response.json()


@app.post("/snowflake/execute")
def execute_snowflake_query(query: QueryRequest):
    """
    Execute a SQL query on Snowflake.
    """
    try:
        connection = engine.connect()
        print("Successfully connected to Snowflake")
        result = connection.execute(query.sql)
        print("Successfully executed query")
        return result.fetchall()
    except Exception as e:
        return {"error": str(e)}
    finally:
        connection.close()
        engine.dispose()


@app.get("/json/download", status_code=200)
def download_json(year: int, quarter: int, background_tasks: BackgroundTasks):
    """
    Download and extract SEC data for a specific year and quarter
    """
    task_id = uuid.uuid4()
    background_tasks.add_task(
        download_task, task_id=task_id, year=year, quarter=quarter
    )
    return {"task_id": task_id}


@app.get("/json/transform", status_code=200)
def transform_json(year: int, quarter: int, background_tasks: BackgroundTasks):
    """
    transform data to JSON
    """
    task_id = uuid.uuid4()
    background_tasks.add_task(
        transform_task, task_id=task_id, year=year, quarter=quarter
    )
    return {"task_id": task_id}


@app.get("/json/load", status_code=200)
def load_json(year: int, quarter: int):
    """
    Load JSON data into Snowflake
    """
    try:
        load_data(year=year, quarter=quarter)
        return {"status": "success"}
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))


@app.get("/json/cleanup", status_code=200)
def cleanup_json():
    """
    Cleanup data
    """
    try:
        shutil.rmtree(Path("./data"), ignore_errors=True)
        shutil.rmtree(Path("./exportfiles"), ignore_errors=True)
    except Exception as e:
        return HTTPException(status_code=500, detail=str(e))


@app.get("/task", status_code=200)
def get_task(task_id: uuid.UUID):
    """
    Get task status
    """
    return {"status": tasks[task_id].status if task_id in tasks else "not found"}


@app.get("/longrunningtask", status_code=200)
def create_long_running_task(duration: int, background_tasks: BackgroundTasks):
    """
    Long running task
    """
    task_id = uuid.uuid4()
    background_tasks.add_task(long_running_task, task_id=task_id, duration=duration)
    return {"task_id": task_id}


def long_running_task(task_id: uuid.UUID, duration: int):
    """
    Long running task
    """
    tasks[task_id] = task(name="long_running_task", status="running")
    time.sleep(duration)
    tasks[task_id] = task(name="long_running_task", status="success")
