from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from commo

with DAG(
    dag_id="dags_python_operator",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2025, 4, 22, tz="Asia/Seoul"),
    catchup=False
) as dag:
