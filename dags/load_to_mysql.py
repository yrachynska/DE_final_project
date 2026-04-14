import pymysql
pymysql.install_as_MySQLdb()
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime
import pandas as pd


def load_oltp_database():
    df = pd.read_csv('/usr/local/airflow/include/applicant.csv')
    mysql_hook = MySqlHook(mysql_conn_id='mysql_oltp')
    engine = mysql_hook.get_sqlalchemy_engine()

    df.to_sql('applicants', con=engine, if_exists='replace', index=False)
    print(f"Successfully loaded MySQL: {len(df)} rows.")


default_args = {
    "owner": "airflow",
    "start_date": datetime(2000, 1, 1),
}

with DAG(
        "mysql_database_load",
        default_args=default_args,
        schedule=None,
        catchup=False,
) as dag:
    t_seed = PythonOperator(
        task_id="load_oltp_database",
        python_callable=load_oltp_database
    )