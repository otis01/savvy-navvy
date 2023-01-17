""" Scheduler file for Airflow """

from datetime import datetime, timedelta
import os
import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from modules.core import download_zip_file
from modules.core import process_geodata
from modules.core import delete_files
from modules.core import upload_to_s3

REDSHIFT_AIRFLOW_CONNECTION: str = "redshift-airflow"
S3_CONNECTION: str = "s3-airflow"

local_tz: pendulum.timezone = pendulum.timezone("Europe/Amsterdam")
root_dir = os.path.dirname(os.path.realpath(__file__))


dag: DAG = DAG(
    dag_id="process_geodata",
    schedule_interval="5 6 * * *",
    max_active_runs=1,
    start_date=datetime(2021, 8, 1, tzinfo=local_tz)
)

with dag:

    task_start = EmptyOperator(
        task_id="start",
    )

    task_end = EmptyOperator(
        task_id="end",
    )

    task_download_files = PythonOperator(
        task_id='download_files',
        python_callable=download_zip_file
    )

    task_process_geodata = PythonOperator(
        task_id="process_geodata",
        python_callable=process_geodata
    )

    task_upload_to_s3 = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_to_s3
    )

    task_insert_records = PostgresOperator(
        task_id="insert_records",
        sql="copy_data.sql"
    )

    task_delete_files = PythonOperator(
        task_id="delete_files",
        python_callable=delete_files
    )
