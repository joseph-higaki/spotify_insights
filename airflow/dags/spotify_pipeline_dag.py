from airflow import DAG
from airflow.operators.python import PythonOperator
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
    dag_id="spotify_pipeline_dag",
    default_args=default_args,
    description="Spotify ingestion and transformation pipeline",
    schedule_interval=None,  # Manually triggered
    start_date=days_ago(1),
    catchup=False,
) as dag:
    def delete_spotify_raw_processed_files(processed_files):
        print(f"delete {processed_files}")
        

    def extract_spotify_raw_files(source_path, gcs_bucket):
        extractor = SpotifyFolderDataExtractor(
            source_path=source_path,
            gcs_bucket=gcs_bucket
        )
        return extractor.process_files()

    def transform_spotify_raw_json_parquet(gcs_bucket, source_path, destination_path):
        transformer = SpotifyJsonToParquetTransformer(
            gcs_bucket=gcs_bucket,
            source_path=source_path,
            destination_path=destination_path        
        )
        return transformer.transform_json_to_parquet()

    # Task 1: Extract JSON files to GCS
    extract_spotify_raw_files_task = PythonOperator(
        task_id="extract_spotify_raw_files",
        python_callable=extract_spotify_raw_files,
        op_kwargs={
            "source_path": os.getenv("SPOTIFY_SOURCE_PATH"),  
            "gcs_bucket": os.getenv("SPOTIFY_GCS_BUCKET"),
        },
    )

    # Task 2: Cleared processed files
    delete_spotify_raw_processed_files_task = PythonOperator(
        task_id="delete_spotify_raw_processed_files",
        python_callable=delete_spotify_raw_processed_files,
        op_kwargs={
            "processed_files": ["{{ task_instance.xcom_pull(task_ids='extract_raw_task', key='return_value') }}"]
        }
    )

    # Task 2: Transform to Parquet
    transform_spotify_raw_json_parquet_task = PythonOperator(
        task_id="transform_spotify_raw_json_parquet",
        python_callable=transform_spotify_raw_json_parquet,
        op_kwargs={
            "gcs_bucket": os.getenv("SPOTIFY_GCS_BUCKET"),
            "source_path": os.getenv("SPOTIFY_RAW_JSON_RELATIVE_PATH"),
            "destination_path": os.getenv("SPOTIFY_RAW_PARQUET_RELATIVE_PATH")
        }
    )
  
    # Set task dependencies
    extract_spotify_raw_files_task >> delete_spotify_raw_processed_files_task
    extract_spotify_raw_files_task >> transform_spotify_raw_json_parquet_task

# ********************************************************************************************
# TO-DO: proper package management https://github.com/joseph-higaki/spotify_insights/issues/26
# ********************************************************************************************
SCRIPT_PATHS_ENV_VAR_NAMES = ["EXTRACT_SCRIPTS_PATH", "TRANSFORM_SCRIPTS_PATH"]

for script_path_var_name in SCRIPT_PATHS_ENV_VAR_NAMES:
    scripts_path = os.getenv(script_path_var_name) # used from container
    if not scripts_path:
        from dotenv import load_dotenv, find_dotenv
        a = find_dotenv()
        load_dotenv(a) #find adjacent .env
        scripts_path = os.getenv(script_path_var_name) # used from local
    print(f"{script_path_var_name} = {scripts_path}") 
    sys.path.insert(0, scripts_path)

# Import the functions to be executed
from spotify_data_extractor import SpotifyFolderDataExtractor
from transform_json_raw_parquet import SpotifyJsonToParquetTransformer
# ********************************************************************************************
# ********************************************************************************************