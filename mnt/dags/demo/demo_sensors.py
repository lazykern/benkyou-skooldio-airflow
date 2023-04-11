from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.utils import timezone

default_args = {"owner": "phusitsom", "start_date": timezone.datetime(2023, 4, 1)}

with DAG("demo_sensors", default_args=default_args, schedule_interval=None) as dag:
    is_file_available = FileSensor(
        task_id="is_file_available",
        fs_conn_id="fs",
        filepath="/opt/airflow/dags/data/hello.txt",
        poke_interval=5,
        timeout=100,
    )
