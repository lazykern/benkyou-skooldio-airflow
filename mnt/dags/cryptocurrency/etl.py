import csv
import logging
from datetime import datetime

import ccxt
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fetch_ohlcv(**context):
    ds = context["ds"]

    exchange = ccxt.binance()

    dt_obj = datetime.strptime(ds, "%Y-%m-%d")
    millis = int(dt_obj.timestamp()) * 1000

    ohlcv = exchange.fetch_ohlcv("SHIB/USDT", "1h", millis, 24)

    logging.info(f"Fetched OHLCV of SHIB/USDT\n{ohlcv}")

    filename = f"shib-{ds}.csv"

    context["ti"].xcom_push(key="filename", value=filename)

    with open(filename, "w") as f:
        csv_writer = csv.writer(f)
        csv_writer.writerow(
            ["timestamp", "open", "highest", "lowest", "closing", "volume"]
        )
        csv_writer.writerows(ohlcv)

    s3_hook = S3Hook(aws_conn_id="minio")
    s3_hook.load_file(
        filename,
        key=f"cryptocurrency/{ds}/shib.csv",
        bucket_name="datalake",
        replace=True,
    )


def download_file(**context):
    ds = context["ds"]

    s3_hook = S3Hook(aws_conn_id="minio")
    file_name = s3_hook.download_file(
        key=f"cryptocurrency/{ds}/shib.csv",
        bucket_name="datalake",
    )
    logging.info(f"Downloaded file from S3\n{file_name}")

    return file_name


def load_data_into_db(**context):
    filename = context["ti"].xcom_pull(task_ids="download_file", key="return_value")

    PostgresHook(postgres_conn_id="postgres").copy_expert(
        """
        COPY cryptocurrency_import (timestamp, open, highest, lowest, closing, volume)
        FROM STDIN WITH CSV HEADER
        """,
        filename,
    )
