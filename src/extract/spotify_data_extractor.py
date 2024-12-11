
import os
import json
import logging
from datetime import datetime
#from google.cloud import storage

class SpotifyFolderDataExtractor:
    def __init__(self, source_path, gcs_bucket):
        """
        Initialize Spotify Data Extraction process
        
        Args:
            source_path (str): Directory with Spotify JSON files
            gcs_bucket (str): GCS Bucket for storing raw data
            username (str): Spotify username
        """
        self.source_path = source_path
        self.gcs_bucket = gcs_bucket        
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
                gcs_bucket=self.gcs_bucket, 
                username=username)
            processed_files.extend(extractor.process_files())
        return processed_files

    def delete_processed_files(self, processed_files):
        for file in processed_files:
            self.logger.info(f"Delete: {file['full_path']}")    

class SpotifyUserDataExtractor:
    def __init__(self, source_path, gcs_bucket, username):
        """
        Initialize Spotify Data Extraction process
        
        Args:
            source_path (str): Directory with Spotify JSON files
            gcs_bucket (str): GCS Bucket for storing raw data
            username (str): Spotify username
        """
        self.source_path = source_path
        #self.gcs_client = storage.Client()
        #self.bucket = self.gcs_client.bucket(gcs_bucket)
        self.username = username
        
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}.{username}")        
    
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

                #Assume daily 
                snapshot_date = processed_at.strftime('%Y-%m-%d')
                
                # Construct GCS destination path
                gcs_path = f"raw/{self.username}/{snapshot_date}/{basename}"
                
                try:
                    #blob = self.bucket.blob(gcs_path)
                    #blob.upload_from_filename(full_path)
                    
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
    gcs_bucket= os.getenv("SPOTIFY_GCS_BUCKET")

    extractor = SpotifyFolderDataExtractor(
        source_path=source_path,
        gcs_bucket=gcs_bucket        
    )
    result = extractor.process_files()
    print(json.dumps(result, indent=2))

    extractor.delete_processed_files(result)

if __name__ == "__main__":
    main()

