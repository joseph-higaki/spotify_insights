import os
#import pandas as pd
#from datetime import datetime
#from google.cloud import storage
import logging

class SpotifyJsonToParquetTransformer:
    def __init__(self, gcs_bucket, source_path, destination_path):
        """
        Initialize Spotify Data Extraction process
        
        Args:
            source_path (str): Directory with Spotify JSON files
            gcs_bucket (str): GCS Bucket for storing raw data
            username (str): Spotify username
        """
        self.gcs_bucket = gcs_bucket  
        self.source_path = source_path
        self.destination_path = destination_path              
        logging.basicConfig(level=logging.INFO)        
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def transform_json_to_parquet(self):
        self.logger.info(f" {self.gcs_bucket}   {self.source_path} {self.destination_path}")
        #for root, user_name, files in os.walk(source_bucket):
            #for file in files:
                #print(f"each file is {file}")

                    
def main():
    #extract params from args or environment specific vars
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 

    gcs_bucket= os.getenv("SPOTIFY_GCS_BUCKET")
    source_path= os.getenv("SPOTIFY_RAW_JSON_RELATIVE_PATH")    
    destination_path= os.getenv("SPOTIFY_RAW_PARQUET_RELATIVE_PATH")    

    transformer = SpotifyJsonToParquetTransformer(        
        gcs_bucket=gcs_bucket,
        source_path=source_path,
        destination_path=destination_path        
    )
    transformer.transform_json_to_parquet()

if __name__ == "__main__":
    main()
