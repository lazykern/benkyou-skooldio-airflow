import logging

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator
from airflow.utils import timezone


def _push_values_to_xcom(**context):
    ti: TaskInstance = context["ti"]
    ti.xcom_push(key="course", value="Airflow (XCom push)")

    return "Airflow (return)"


def _pull_values_from_xcom(**context):
    value1 = context["ti"].xcom_pull(task_ids="push_values_to_xcom", key="course")
    value2 = context["ti"].xcom_pull(task_ids="push_values_to_xcom", key="return_value")

    logging.info(f"value1: {value1}")
    logging.info(f"value2: {value2}")


default_args = {
    "owner": "phusitsom",
    "start_date": timezone.datetime(2023, 4, 1),
}
with DAG(
    "demo_xcoms",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    push_values_to_xcom = PythonOperator(
        task_id="push_values_to_xcom",
        python_callable=_push_values_to_xcom,
    )

    pull_values_from_xcom = PythonOperator(
        task_id="pull_values_from_xcom",
        python_callable=_pull_values_from_xcom,
    )

    push_values_to_xcom >> pull_values_from_xcom
