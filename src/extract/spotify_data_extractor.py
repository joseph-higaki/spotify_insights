
import os
import json
import logging
from datetime import datetime
from google.cloud import storage

class SpotifyFolderDataExtractor:
    def __init__(self, source_path, bucket_name, destination_path):
        """
        Initialize Spotify Data Extraction process
        
        Args:
            source_path (str): Directory with Spotify JSON files
            bucket_name (str): Bucket for storing raw data
            destination_path (str): path within the Bucket for storing raw data            
        """
        self.source_path = source_path
        self.bucket_name = bucket_name
        self.destination_path = destination_path
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")        

    def process_files(self):
        """
        Walk username folders
        """
        processed_files = []
        for username in os.listdir(self.source_path):            
            extractor = SpotifyUserDataExtractor(
                source_path=os.path.join(self.source_path, username),
                bucket_name=self.bucket_name, 
                destination_path = self.destination_path,
                username=username)
            processed_files.extend(extractor.process_files())
        return processed_files

    def delete_processed_files(self, processed_files):
        for file in processed_files:
            self.logger.info(f"Delete: {file['full_path']}")    

class SpotifyUserDataExtractor:
    def __init__(self, source_path, bucket_name, destination_path, username):
        """
        Initialize Spotify Data Extraction process
        
        Args:
            source_path (str): Directory with Spotify JSON files
            bucket_name (str): Bucket for storing raw data
            destination_path (str): path within the Bucket for storing raw data
            username (str): Spotify username
        """
        self.source_path = source_path        
        self.bucket_name = bucket_name
        self.destination_path = destination_path
        self.username = username
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{username}")        

    
    def upload_to_gcs(self, local_full_path:str, local_basename: str, snapshot_date: str):
        self.logger.info(f"bucket_name: {self.bucket_name}")        
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)

        destination_blob_name = f"{self.destination_path}/{self.username}/{snapshot_date}/{local_basename}"         
        blob = bucket.blob(destination_blob_name)
        generation_match_precondition = 0
        blob.upload_from_filename(local_full_path, if_generation_match=generation_match_precondition)
        self.logger.info(f"Uploaded succesfully: {self.bucket_name} - {destination_blob_name}")        
        return 0 


    def process_files(self):
        """
        Process Spotify JSON files with idempotence and incremental loading
        """
        processed_files = []        
        for basename in os.listdir(self.source_path):
            if basename.endswith('.json'):
                full_path = os.path.join(self.source_path, basename)
                
                # Generate unique snapshot date from file metadata
                file_stats = os.stat(full_path)
                created_at = datetime.fromtimestamp(
                    #file_stats.st_ctime
                    # Grabs stat.ST_MTIME Time of last modification.
                    # Because using stat.ST_CTIME s usually overriden when copying or moving files from one path to another
                    file_stats.st_mtime 
                )
                processed_at = datetime.now()

                #group by daily 
                snapshot_date = processed_at.strftime('%Y-%m-%d')
                
                # Construct bucket destination path
                #bucket_full_path = f"{self.bucket_name}{bucket_full_path}/{self.username}/{snapshot_date}/{basename}"
                
                try:
                    #blob = self.bucket.blob(gcs_path)
                    #blob.upload_from_filename(full_path)
                    self.upload_to_gcs(
                        local_full_path=full_path,
                        local_basename=basename,
                        snapshot_date=snapshot_date
                    )
                    
                    processed_files.append({
                        'username': self.username,
                        'full_path': full_path,
                        'basename': basename,
                        'created_at': created_at.isoformat(),
                        'processed_at': processed_at.isoformat(),
                        'snapshot_date': snapshot_date
                    })
                    
                    self.logger.info(f"Processed: {full_path}")
                    
                    #update metadata
                    #delete from source

                except Exception as e:
                    self.logger.error(f"Error processing {full_path}: {e}")
        
        return processed_files

def main():
    #extract params from args or environment specific vars
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 

    source_path= os.getenv("SPOTIFY_SOURCE_PATH")
    bucket_name= os.getenv("SPOTIFY_BUCKET")
    destination_path = os.getenv("SPOTIFY_RAW_JSON_RELATIVE_PATH")
    

    extractor = SpotifyFolderDataExtractor(
        source_path=source_path,
        bucket_name=bucket_name,
        destination_path = destination_path     
    )
    result = extractor.process_files()
    print(json.dumps(result, indent=2))

    extractor.delete_processed_files(result)

if __name__ == "__main__":
    main()

