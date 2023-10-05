import os

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from web.operators.assignment.webToGCS import WebToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator


AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

SCHEMA = schema = [
        {"name": "VendorID", "type": "INT64", "mode": "NULLABLE"},
        {"name": "lpep_pickup_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "lpep_dropoff_datetime", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "store_and_fwd_flag", "type": "STRING", "mode": "NULLABLE"},
        {"name": "RatecodeID", "type": "INT64", "mode": "NULLABLE"},
        {"name": "PULocationID", "type": "INT64", "mode": "NULLABLE"},
        {"name": "DOLocationID", "type": "INT64", "mode": "NULLABLE"},
        {"name": "passenger_count", "type": "INT64", "mode": "NULLABLE"},
        {"name": "trip_distance", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "fare_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "trip_type", "type": "INT64", "mode": "NULLABLE"},
        {"name": "ehail_fee", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "extra", "type": "INT64", "mode": "NULLABLE"},
        {"name": "mta_tax", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "tip_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "tolls_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "improvement_surcharge", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "total_amount", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "payment_type", "type": "INT64", "mode": "NULLABLE"},
        {"name": "congestion_surcharge", "type": "FLOAT64", "mode": "NULLABLE"},
        {"name": "season", "type": "STRING", "mode": "NULLABLE"},
    ]

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
DATASET="airflow_nyc"

with DAG(
    dag_id="Assignment1-Load-Green-Yello-FHV-Tax-Data-To-GCS-To-BQ",
    description="Download Taxi Data to GCS Bucket and then to BigQuery",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 2 * *",
    max_active_runs=1,
    catchup=True,
    tags=["Website-to-GCS-Bucket-To-BigQuery"],
) as dag:
    start = EmptyOperator(task_id="start")


    download_to_gcs = WebToGCSOperator(
        task_id="download_to_gcs",
        base_endpoint=ENDPOINT,
        destination_bucket=GCP_BUCKET,
        service=SERVICE,
    )
    
    source_objects = [f"{SERVICE}/{SERVICE}_tripdata_{{ dag_run.logical_date.strftime(\'%Y-%m\') }}.csv"]
    load_gcs_to_bgquery = GCSToBigQueryOperator(
        task_id = f"load_gcs_to_bgquery",
        bucket=f"{GCP_BUCKET}",
        source_objects=source_objects,
        destination_project_dataset_table=f"{DATASET}.{SERVICE}_{DATASET}_data", # `airflow_nyc.green_dataset_data` i.e table name
        autodetect=True, #DETECT SCHEMA : the columns and the type of data in each columns of the CSV file
        # schema_fields=SCHEMA, # SCHEMA : the columns and the type of data in each columns of the CSV file
        write_disposition="WRITE_APPEND", # command to update table from the  latest (or last row) row number upon every job run or task run
    )

    end = EmptyOperator(task_id="end")

    start >> download_to_gcs >> load_gcs_to_bgquery >> end