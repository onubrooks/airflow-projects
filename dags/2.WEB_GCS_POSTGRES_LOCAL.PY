import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from web.operators.assignment.webToGCS import WebToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.operators.python import PythonOperator
from utils.dbingestion import db_conn_ingestion


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

DEFAULT_ARGS = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "email": [os.getenv("ALERT_EMAIL", "")],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
GCP_BUCKET = os.environ.get("GCP_GCS_BUCKET")
ENDPOINT = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
SERVICE = "green"
FILE_PATH = "tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv"
SAVE_FILE_PATH = f"{AIRFLOW_HOME}/{FILE_PATH}"
TABLE_NAME = "green_taxi_data"

with DAG(
    dag_id="Assignment2-Load-Green-Yello-FHV-Tax-Data-To-GCS-To-LocalPG",
    description="Download Taxi Data to GCS Bucket and then to Local Postgres Server",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    catchup=True,
    tags=["Website-to-GCS-Bucket-To-LocalPG"],
) as dag:
    start = EmptyOperator(task_id="start")


    upload_to_gcs = WebToGCSOperator(
        task_id="download_to_gcs",
        base_endpoint=ENDPOINT,
        destination_bucket=GCP_BUCKET,
        service=SERVICE
    )
    
    download_from_gcs = GCSToLocalFilesystemOperator(
		task_id="download_file_from_gcs",
		object_name=f"{SERVICE}/{SERVICE}_{FILE_PATH}",
		bucket=GCP_BUCKET,
		filename=SAVE_FILE_PATH
    )
    ingest_data_to_postgres = PythonOperator(
        task_id ="load_data_to_PD_DB",
        python_callable=db_conn_ingestion,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME,
            csv_file=SAVE_FILE_PATH
        )
    )
    delete_local_file = BashOperator(
      	task_id = "delete_file",
      	bash_command = f'rm {SAVE_FILE_PATH}'
    )

    end = EmptyOperator(task_id="end")

    start >> upload_to_gcs >> download_from_gcs >> ingest_data_to_postgres >> delete_local_file >> end