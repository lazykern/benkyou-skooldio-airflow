import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone


def _query_data():
    pg_hook = PostgresHook(
        postgres_conn_id="airflow_metastore",
        schema="airflow",
    )
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    sql = """
    SELECT dag_id, owners FROM dag
    """

    cursor.execute(sql)
    rows = cursor.fetchall()

    for row in rows:
        logging.info(row)


default_args = {
    "owner": "phusitsom",
    "start_date": timezone.datetime(2023, 4, 1),
}
with DAG(
    "demo_hooks",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    query_data = PythonOperator(
        task_id="query_data",
        python_callable=_query_data,
    )
