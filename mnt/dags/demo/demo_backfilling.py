from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils import timezone

default_args = {
    "owner": "phusitsom",
    "start_date": timezone.datetime(2023, 4, 1),
}

with DAG(
    "demo_backfilling",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:
    echo_ds = BashOperator(
        task_id="echo_ds",
        bash_command="echo {{ ds }}",
    )

    echo_logical_date = BashOperator(
        task_id="echo_logical_date",
        bash_command="echo {{ logical_date }}",
    )

    echo_ds >> echo_logical_date
