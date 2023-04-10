import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _hello():
    logging.info("Hello World")


def _world():
    logging.info("Hello World")


default_args = {"owner": "phusitsom", "start_date": timezone.datetime(2023, 4, 10)}

with DAG(
    dag_id="demo_dependencies", default_args=default_args, schedule_interval="@daily"
) as dag:
    hello_task = PythonOperator(
        task_id="hello",
        python_callable=_hello,
    )

    world_task = PythonOperator(
        task_id="world",
        python_callable=_world,
    )

    hello_task >> world_task
