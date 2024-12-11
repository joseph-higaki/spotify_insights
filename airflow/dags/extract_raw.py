import os
import json
import logging
from datetime import datetime

from google.cloud import storage
import pandas as pd

# Constants
LOCAL_DIR = "/workspaces/spotify_insights/0_tmp_data/new"
GCS_BUCKET = "spotify-insights"

RAW_BUCKET_PREFIX = "raw/"

# Initialize logging
logging.basicConfig(level=logging.INFO)

# Upload file to GCS
def upload_to_gcs(bucket_name, local_file_path, gcs_path):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_file_path)
    logging.info(f"Uploaded {local_file_path} to {gcs_path}")

# Process JSON file
def process_json_file(file_path, last_max_ts):
    with open(file_path, 'r') as file:
        streams = json.load(file)

    # Filter streams based on last_max_ts
    filtered_streams = [stream for stream in streams if stream["ts"] > last_max_ts]
    if not filtered_streams:
        logging.info(f"No new streams found in {file_path}. Skipping.")
        return None, None

    # Deduplicate and get max_ts
    df = pd.DataFrame(filtered_streams)
    df = df.drop_duplicates(subset=["ts"])
    max_ts = df["ts"].max()

    # Save filtered streams to a temporary file
    temp_file = f"{file_path}.filtered.json"
    df.to_json(temp_file, orient="records", lines=False)
    return temp_file, max_ts

# Main Function
def extract_raw():       
    print("extract_raw task")
    print("walk through usernames folders")
    print("for each fodler content, process json")
    print("extract metadata")
    print("upload to gcs")
    print("store in metadata store")



