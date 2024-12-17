from io import BytesIO
import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import logging
from typing import Generator


class SpotifyJsonToParquetTransformer:
    def __init__(self, source_bucket_name, source_path, destination_bucket_name, destination_path):
        """
        Initialize Spotify Data Extraction process
        
        Args:            
            source_bucket_name (str): Bucket for reading the raw data from 
            source_path (str): path for source raw files
            destination_bucket_name (str): Bucket to store the parquet data to
            destination_path (str): path for parquet  files            
        """
        self.source_bucket_name = source_bucket_name  
        self.source_path = source_path
        self.destination_bucket_name = destination_bucket_name
        self.destination_path = destination_path              
        logging.basicConfig(level=logging.INFO)        
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")


    def get_blobname_components(self, blob_name):    
        def get_safe(list, index, default=''): 
            return list[index] if len(list) > abs(index) else default    
        components =blob_name.split('/')
        username = get_safe(components, -3) 
        date = get_safe(components, -2) 
        filename = get_safe(components, -1)         
        message = "Processing requires path form: /folders..../<username>/<date>/<filename>"
        if filename == "":
            self.logger.error(f"{message}. Filename is empty")
        elif username == "" or date == "":
            self.logger.warning(f"{message}. username '{username}' or date '{date}' is empty")
        return (username, date, filename)

    def iterate_files_from_gcs(self):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.source_bucket_name)
        for blob_item in bucket.list_blobs(prefix=self.source_path):                
            username, date, filename = self.get_blobname_components(blob_item.name)
            yield (username, date, filename, blob_item.download_as_string())
        
    def get_destination_full_path(self) -> str:
        return f"gs://{self.destination_bucket_name}/{self.destination_path}"

    def transform_json_to_parquet(self):
        self.logger.info(f" {self.source_bucket_name}   {self.source_path} {self.destination_path}")
        for username, date, filename, json_content in self.iterate_files_from_gcs():
            if json_content:
                df = pd.read_json(BytesIO(json_content), dtype=str) # Force everything as str, do data type discovery later
                df['username'] = username
                df['snapshot_date'] = date
                table = pa.Table.from_pandas(df)
                destination_full_path = self.get_destination_full_path()                
                pq.write_to_dataset(
                    table,
                    root_path=destination_full_path,
                    partition_cols=['username', 'snapshot_date']
                )   

                    
def main():
    #extract params from args or environment specific vars
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 

    source_bucket_name= os.getenv("SPOTIFY_SOURCE_BUCKET")
    source_path= os.getenv("SPOTIFY_RAW_JSON_RELATIVE_PATH")    
    destination_bucket_name= os.getenv("SPOTIFY_PIPELINE_BUCKET")
    destination_path= os.getenv("SPOTIFY_RAW_PARQUET_RELATIVE_PATH")    

    transformer = SpotifyJsonToParquetTransformer(        
        source_bucket_name=source_bucket_name,
        source_path=source_path,
        destination_bucket_name=destination_bucket_name,
        destination_path=destination_path        
    )
    transformer.transform_json_to_parquet()

if __name__ == "__main__":
    main()
