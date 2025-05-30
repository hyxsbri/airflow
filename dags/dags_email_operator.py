from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2025, 4, 16, tz="Asia/Seoul"),
    catchup=False
) as dag:
     send_email_task = EmailOperator(
          task_id='send_email_task',
          to='gydnjs1231@naver.com',
          subject='Airflow succeeded.',
          html_content='Upgrade completed.'
     )