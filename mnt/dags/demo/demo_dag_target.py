import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def __log_message(**context):
    dag_run = context.get("dag_run")
    conf = dag_run.conf
    message = conf.get("message")
    logging.info(f"Remotely received value: {message}")


default_args = {
    "owner": "phusitsom",
    "start_date": timezone.datetime(2023, 4, 10),
}
with DAG(
    "demo_context",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    demo_dag_target = PythonOperator(
        task_id="demo_dag_target", python_callable=__log_message
    )
