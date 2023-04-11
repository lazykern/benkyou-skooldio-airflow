from datetime import timedelta
from time import sleep

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "phusitsom",
    "email": "phusitsom@phusitsom.me",
    "start_date": pendulum.datetime(2023, 4, 1, tz="Asia/Bangkok"),
    "sla": timedelta(seconds=5),
}

with DAG(
    "demo_sla",
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
) as dag:
    sleep_task = PythonOperator(task_id="sleep", python_callable=lambda: sleep(3))
    sleep_sla_task = PythonOperator(
        task_id="sleep_sla",
        python_callable=lambda: sleep(3),
        sla=timedelta(seconds=30),
    )

    sleep_task >> sleep_sla_task
