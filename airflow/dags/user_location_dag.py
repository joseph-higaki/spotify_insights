from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import sys
import pandas as pd

# Default arguments for the DAG
default_args = {    
    "depends_on_past": False    
}

# Define the DAG
with DAG(
    dag_id="user_location_dag_2",
    default_args=default_args,
    description="Spotify ingestion and transformation pipeline - User Location",
    schedule_interval=None,  # Manually triggered
    start_date=days_ago(1),
    catchup=False,
) as dag:
    

    def extract_year_coordinates(airport_location_path: str, user_location_path: str, years_location_path: str):
        extractor = UserLocationYearCoordinatesExtractor(airport_location_path, user_location_path, years_location_path)
        extractor.extract()

    def extract_weather_data(years_location_path: str, destination_bucket_name: str, destination_path: str):
        year_coordinates = pd.read_csv(years_location_path)
        year_coordinates.apply(lambda x:   
                    WeatherDataExtractor(x['year'], x['latitude'], x['longitude'], 
                                         destination_bucket_name, 
                                         destination_path
                                         ).extract(), axis=1)          


    upload_user_location_files_task = LocalFilesystemToGCSOperator(
            task_id="upload_user_location_files",
            src=os.getenv("USER_LOCATION_SOURCE_PATH")+"/*",
            dst=os.getenv("USER_LOCATION_DESTINATION_PATH") + "/",
            bucket=os.getenv("USER_LOCATION_DESTINATION_BUCKET")
        )

    upload_airport_code_files_task = LocalFilesystemToGCSOperator(
            task_id="upload_airport_code_files",
            src=os.getenv("AIRPORT_CODE_LOCATION_SOURCE_PATH"),
            dst=os.getenv("AIRPORT_CODE_LOCATION_DESTINATION_PATH"),
            bucket=os.getenv("AIRPORT_CODE_LOCATION_DESTINATION_BUCKET")
        )
    
    extract_year_coordinates_task = PythonOperator( 
        task_id="extract_year_coordinates",
        python_callable=extract_year_coordinates,
        op_kwargs={
            "airport_location_path": f"gs://{os.getenv('AIRPORT_CODE_LOCATION_DESTINATION_BUCKET')}/{os.getenv('AIRPORT_CODE_LOCATION_DESTINATION_PATH')}",
            "user_location_path": f"gs://{os.getenv('USER_LOCATION_DESTINATION_BUCKET')}/{os.getenv('USER_LOCATION_DESTINATION_PATH')}",
            "years_location_path": f"gs://{os.getenv('YEARS_LOCATION_DESTINATION_BUCKET')}/{os.getenv('YEARS_LOCATION_LOCATION_DESTINATION_PATH')}"

        }
    )

    extract_weather_data_task = PythonOperator(
        task_id="extract_weather_data",
        python_callable=extract_weather_data,
        op_kwargs={
            "years_location_path": f"gs://{os.getenv('YEARS_LOCATION_DESTINATION_BUCKET')}/{os.getenv('YEARS_LOCATION_LOCATION_DESTINATION_PATH')}",
            "destination_bucket_name": os.getenv("WEATHER_DESTINATION_BUCKET"),
            "destination_path": os.getenv("WEATHER_DESTINATION_PATH")
        }
    )

    upload_user_location_files_task >> extract_year_coordinates_task
    upload_airport_code_files_task >> extract_year_coordinates_task
    extract_year_coordinates_task >> extract_weather_data_task


    
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
from user_location_year_coordinates_extractor import UserLocationYearCoordinatesExtractor
from weather_data_extractor import WeatherDataExtractor
# ********************************************************************************************
# ********************************************************************************************
