import pymysql
pymysql.install_as_MySQLdb()
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import duckdb
import json
import os

JSON_LEADS_DIR = '/usr/local/airflow/data/leads_data'
JSON_CALLS_DIR = '/usr/local/airflow/data/calls_data'
DUCKDB_PATH = '/usr/local/airflow/data/marketing.duckdb'


def load_leads_to_raw():
    last_time = Variable.get("last_loaded_lead_time", default_var='2000-01-01T00:00:00')

    files = [f for f in os.listdir(JSON_LEADS_DIR)]

    new_files = []

    latest_date_in_batch = last_time

    for f in files:
        file_path = os.path.join(JSON_LEADS_DIR, f)

        with open(file_path, "r") as file:
            data = json.load(file)
            file_date = data.get('date')
            if file_date > last_time:
                new_files.append(data)

                if file_date > latest_date_in_batch:
                    latest_date_in_batch = file_date

    df = pd.DataFrame(new_files)

    if len(df) > 0:
        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("CREATE TABLE IF NOT EXISTS raw_leads AS SELECT * FROM df WHERE 1=0")
        conn.execute("INSERT INTO raw_leads SELECT * FROM df")

        Variable.set("last_loaded_lead_time", latest_date_in_batch)
        print(f"{len(df)} leads loaded to raw_leads")
        conn.close()
    else:
        print(f"No new leads to load")



def load_calls_to_raw():
    last_time = Variable.get("last_loaded_calls_time", default_var='2000-01-01T00:00:00')

    files = [f for f in os.listdir(JSON_CALLS_DIR)]

    new_files = []

    latest_date_in_batch = last_time

    for f in files:
        file_path = os.path.join(JSON_CALLS_DIR, f)

        with open(file_path, "r") as file:
            data = json.load(file)
            file_time = data.get('time')
            if file_time > last_time:
                new_files.append(data)

                if file_time > latest_date_in_batch:
                    latest_date_in_batch = file_time

    df = pd.DataFrame(new_files)

    if len(df) > 0:
        conn = duckdb.connect(DUCKDB_PATH)
        conn.execute("CREATE TABLE IF NOT EXISTS raw_calls AS SELECT * FROM df WHERE 1=0")
        conn.execute("INSERT INTO raw_calls SELECT * FROM df")

        Variable.set("last_loaded_calls_time", latest_date_in_batch)
        print(f"{len(df)} calls loaded to raw_calls")
        conn.close()
    else:
        print(f"No new calls to load")



default_args = {
    "owner": "airflow",
    "start_date": datetime(2000, 1, 1),
    "retries": 2,
    "catchup": False
}

dag = DAG(
    "json_pipeline",
    default_args=default_args,
    schedule="@hourly",
    catchup=False,
)

load_leads_to_raw_task = PythonOperator(
    task_id="load_leads_to_raw",
    python_callable=load_leads_to_raw,
    dag=dag
)

load_calls_to_raw_task = PythonOperator(
    task_id="load_calls_to_raw",
    python_callable=load_calls_to_raw,
    dag=dag
)