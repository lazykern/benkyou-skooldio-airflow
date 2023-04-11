from datetime import timedelta

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils import timezone
from cryptocurrency.etl import download_file, fetch_ohlcv, load_data_into_db

default_args = {
    "owner": "phusitsom",
    "start_date": timezone.datetime(2023, 4, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    "cryptocurrency-pipeline", default_args=default_args, schedule_interval=None
) as dag:
    fetch_ohlcv_task = PythonOperator(
        task_id="fetch_ohlcv", python_callable=fetch_ohlcv
    )

    download_file_task = PythonOperator(
        task_id="download_file", python_callable=download_file
    )

    create_import_table_task = PostgresOperator(
        task_id="create_import_table",
        postgres_conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS cryptocurrency_import (
            timestamp BIGINT,
            open FLOAT,
            highest FLOAT,
            lowest FLOAT,
            closing FLOAT,
            volume FLOAT
    )
    """,
    )

    load_data_into_database_task = PythonOperator(
        task_id="load_data_into_database", python_callable=load_data_into_db
    )

    create_final_table_task = PostgresOperator(
        task_id="create_final_table",
        postgres_conn_id="postgres",
        sql="""
        CREATE TABLE IF NOT EXISTS cryptocurrency (
            timestamp BIGINT PRIMARY KEY,
            open FLOAT,
            highest FLOAT,
            lowest FLOAT,
            closing FLOAT,
            volume FLOAT
    )
    """,
    )

    merge_import_into_final_table_task = PostgresOperator(
        task_id="merge_import_into_final_table",
        postgres_conn_id="postgres",
        sql="""
            INSERT INTO cryptocurrency(
               timestamp,
               open,
               highest,
               lowest,
               closing,
               volume
               )
            SELECT 
               timestamp,
               open,
               highest,
               lowest,
               closing,
               volume
            FROM
                cryptocurrency_import
            ON CONFLICT (timestamp)
            DO UPDATE SET
                open = EXCLUDED.open,
                highest = EXCLUDED.highest,
                lowest = EXCLUDED.lowest,
                closing = EXCLUDED.closing,
                volume = EXCLUDED.volume
            """,
    )

    clear_import_table_task = PostgresOperator(
        task_id="clear_import_table",
        postgres_conn_id="postgres",
        sql="""
            DELETE FROM cryptocurrency_import
            """,
    )

    email_notify_task = EmailOperator(
        task_id="email_notify",
        to=["phusitsom@phusitsom.me"],
        subject="Loaded data into database successfully on {{ ds }}",
        html_content="Your pipeline has loaded data into database successfully on {{ ds }}",
    )

    (
        fetch_ohlcv_task
        >> download_file_task
        >> create_import_table_task
        >> load_data_into_database_task
        >> create_final_table_task
        >> merge_import_into_final_table_task
        >> clear_import_table_task
        >> email_notify_task
    )
