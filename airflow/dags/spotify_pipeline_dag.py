from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Import the functions to be executed
from extract_raw import extract_raw
from load_raw_parquet import load_raw_parquet

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

    # Task 1: Extract JSON files to GCS
    extract_raw_task = PythonOperator(
        task_id="extract_raw_files",
        python_callable=extract_raw,
        op_kwargs={
            "source_path": "/workspaces/spotify_insights/0_tmp_data/new",  # Update with your local file path
            "destination_bucket": "gs://spotify-insights/raw/",
        },
    )

    # Task 2: Transform JSON to Parquet
    load_raw_parquet_task = PythonOperator(
        task_id="load_raw_parquet",
        python_callable=load_raw_parquet,
        op_kwargs={
            "source_bucket": "gs://spotify-insights/raw/",
            "destination_bucket": "gs://spotify-insights/raw-tables/",
        },
    )

    # Set task dependencies
    extract_raw_task >> load_raw_parquet_task
