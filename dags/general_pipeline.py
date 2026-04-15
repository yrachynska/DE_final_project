import io
import pymysql
pymysql.install_as_MySQLdb()
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime
import pandas as pd
import duckdb


DUCKDB_PATH = '/usr/local/airflow/data/marketing.duckdb'
MYSQL_CONN = 'mysql_oltp'
MINIO_CONN = 'minio_s3'


def load_mysql_to_raw():
    last_date = Variable.get("last_loaded_applicant_date", default_var='2000-01-01 00:00:00')
    mysql_hook = MySqlHook(mysql_conn_id=MYSQL_CONN)

    sql = f"SELECT * FROM applicants WHERE date > '{last_date}'"
    df = mysql_hook.get_pandas_df(sql)

    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS raw_applicant AS SELECT * FROM df WHERE 1=0")
    conn.execute("INSERT INTO raw_applicant SELECT * FROM df")

    new_last_date = str(df['date'].max())
    Variable.set("last_loaded_applicant_date", new_last_date)

    conn.close()
    print(f"{len(df)} applicants loaded to raw_applicant")


def load_minio_to_raw():
    last_date = Variable.get("last_loaded_advertising_date", default_var='2000-01-01 00:00:00')
    s3 = S3Hook(aws_conn_id=MINIO_CONN)

    file_content = s3.read_key(key='advertising_companies.csv', bucket_name='marketing')
    df = pd.read_csv(io.StringIO(file_content))

    df['date_start'] = pd.to_datetime(df['date_start'])
    df_new = df[df['date_start'] > last_date]

    conn = duckdb.connect(DUCKDB_PATH)
    conn.execute("CREATE TABLE IF NOT EXISTS raw_advertising AS SELECT * FROM df_new WHERE 1=0")
    conn.execute("INSERT INTO raw_advertising SELECT * FROM df_new")

    new_last_date = str(df['date_start'].max())
    Variable.set("last_loaded_advertising_date", new_last_date)

    conn.close()
    print(f"{len(df_new)} advertising companies loaded to raw_advertising")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2000, 1, 1),
    "retries": 2,
    "catchup": False
}

dag = DAG(
    "general_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
)

load_mysql_to_raw_task = PythonOperator(
    task_id="load_mysql_to_raw",
    python_callable=load_mysql_to_raw,
    dag=dag
)

load_minio_to_raw_task = PythonOperator(
    task_id="load_minio_to_raw",
    python_callable=load_minio_to_raw,
    dag=dag
)