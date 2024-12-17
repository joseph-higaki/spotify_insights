from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys


# Default arguments for the DAG
default_args = {    
    "depends_on_past": False    
}

# Define the DAG
with DAG(
    dag_id="user_location_dag",
    default_args=default_args,
    description="Spotify ingestion and transformation pipeline - User Location",
    schedule_interval=None,  # Manually triggered
    start_date=days_ago(1),
    catchup=False,
) as dag:
    upload_user_location_files = LocalFilesystemToGCSOperator(
            task_id="upload_user_location_files",
            src=os.getenv("USER_LOCATION_SOURCE_PATH")+"/*",
            dst=os.getenv("USER_LOCATION_DESTINATION_PATH") + "/",
            bucket=os.getenv("USER_LOCATION_SOURCE_BUCKET")
        )

    upload_airport_code_files = LocalFilesystemToGCSOperator(
            task_id="upload_airport_code_files",
            src=os.getenv("AIRPORT_CODE_LOCATION_SOURCE_PATH"),
            dst=os.getenv("AIRPORT_CODE_LOCATION_DESTINATION_PATH"),
            bucket=os.getenv("USER_LOCATION_SOURCE_BUCKET")
        )
    
    upload_user_location_files
    upload_airport_code_files