import os
import pandas as pd
from datetime import datetime
from google.cloud import storage
import logging

# Constants
GCS_BUCKET = "spotify-insights"
PARQUET_BUCKET_PREFIX = "raw-tables/"
LOCAL_JSON_DIR = "/local/path/to/filtered/json"
LOCAL_PARQUET_DIR = "/local/path/to/parquet"
USER_NAME = "username"

# Upload Parquet file to GCS
def upload_to_gcs(bucket_name, local_file_path, gcs_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_file_path)
    logging.info(f"Uploaded {local_file_path} to {gcs_path}")

# Transform JSON to Parquet
def json_to_parquet(json_file_path, user_name, snapshot_date):
    try:
        # Load JSON data into DataFrame
        df = pd.read_json(json_file_path, orient="records", lines=False)
        
        # Clean up the data if necessary (optional)
        df.fillna({"episode_name": "Unknown", "episode_show_name": "Unknown"}, inplace=True)
        
        # Define Parquet file path
        local_parquet_file = os.path.join(
            LOCAL_PARQUET_DIR, f"{user_name}_{snapshot_date}.parquet"
        )
        
        # Save DataFrame to Parquet
        df.to_parquet(local_parquet_file, engine="pyarrow", index=False)
        logging.info(f"Converted {json_file_path} to {local_parquet_file}")
        
        return local_parquet_file
    except Exception as e:
        logging.error(f"Error converting {json_file_path} to Parquet: {e}")
        return None

# Main function for JSON to Parquet transformation
def load_raw_parquet(source_bucket, destination_bucket):
    snapshot_date = datetime.now().strftime("%Y-%m-%d")
    for root, user_name, files in os.walk(source_bucket):
        for file in files:
            print(f"each file is {file}")
            # local_parquet_file = json_to_parquet(json_file_path, USER_NAME, snapshot_date)
            # if local_parquet_file:
            #     # Upload to GCS
            #     gcs_path = os.path.join(PARQUET_BUCKET_PREFIX, USER_NAME, snapshot_date, os.path.basename(local_parquet_file))
            #     upload_to_gcs(GCS_BUCKET, local_parquet_file, gcs_path)
                    
