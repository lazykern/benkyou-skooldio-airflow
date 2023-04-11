from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils import timezone

default_args = {
    "owner": "phusitsom",
    "start_date": timezone.datetime(2023, 4, 10),
}
with DAG(
    "demo_context",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    demo_dag_trigger = TriggerDagRunOperator(
        task_id="demo_dag_trigger",
        trigger_dag_id="demo_dag_target",
        conf={"message": "Goodbye World"},
    )
