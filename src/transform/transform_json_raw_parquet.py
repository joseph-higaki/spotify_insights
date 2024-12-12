import os
#import pandas as pd
#from datetime import datetime
#from google.cloud import storage
import logging

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

    def transform_json_to_parquet(self):
        self.logger.info(f" {self.source_bucket_name}   {self.source_path} {self.destination_path}")
        #for root, user_name, files in os.walk(source_bucket):
            #for file in files:
                #print(f"each file is {file}")

                    
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
