from io import BytesIO
import os
import hashlib
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage
import logging
import duckdb
from typing import Generator


class SpotifyTypeDedupTransformer:
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

    
    def get_source_full_path(self) -> str:
        return f"gs://{self.source_bucket_name}/{self.source_path}"
    
    def get_destination_full_path(self) -> str:
        return f"gs://{self.destination_bucket_name}/{self.destination_path}"
    

    def normalize_raw_partition_columns(self, df):
        df['username'] = df['username'].astype('str')
        df['snapshot_date'] = pd.to_datetime(df['snapshot_date']).dt.tz_localize('UTC')

    def transform_offline_timestamp(self, df):
        df['offline_timestamp_float'] = df['offline_timestamp'].astype('float64')        
        df['offline_timestamp_rounded'] = pd.to_numeric(df['offline_timestamp_float'], errors='coerce').round().astype('Int64')
        #df['offline_timestamp_rounded'] = df['offline_timestamp_rounded'].astype('Int64')
        # Remove original  columns 
        df.drop(columns=['offline_timestamp', 'offline_timestamp_float'], inplace=True)
        # promote int as official column        
        df.rename(columns={'offline_timestamp_rounded': 'offline_timestamp'}, inplace=True)
    
    def transform_datatypes_timestamp(self, df):
        # Define the desired data types
        dtype_mapping = {
            'ts': 'datetime64[ns, UTC]',  # timestamp
            'platform': 'str',    # str
            'ms_played': 'Int64',    # integer with NaN support (nullable integer type)
            'conn_country': 'str',  # str
            'ip_addr': 'str',  # str
            'master_metadata_track_name': 'str',  # str
            'master_metadata_album_artist_name': 'str',  # str
            'master_metadata_album_album_name': 'str',  # str
            'spotify_track_uri': 'str',  # str
            'episode_name': 'str',  # str
            'episode_show_name': 'str',  # str
            'spotify_episode_uri': 'str',  # str
            'reason_start': 'str',  # str
            'reason_end': 'str',  # str
            'shuffle': 'bool',  # boolean
            'skipped': 'bool',  # boolean
            'offline': 'bool',  # boolean
            'offline_timestamp': 'Int64',  # nullable integer
            'incognito_mode': 'bool',  # boolean
            'username': 'str',  # str
            'snapshot_date': 'datetime64[ns, UTC]'  # date
        }
        for column, dtype in dtype_mapping.items():
            df[column] = df[column].astype(dtype)
    
    def deduplicate(self, df):
        def genMd5Hash(row, info_columns) -> str:
            string = '-'.join([str(row[col]) for col in info_columns])            
            return hashlib.md5(string.encode()).hexdigest()
        def generate_surrogate_key(df, columns):
            df['stream_id'] = df.apply(lambda row: genMd5Hash(row, columns),axis=1)
        
        source_info_columns = ['ts', 'platform', 'ms_played', 'conn_country', 'ip_addr', 
                       'master_metadata_track_name', 'master_metadata_album_artist_name', 
                       'master_metadata_album_album_name', 'spotify_track_uri', 'episode_name', 
                       'episode_show_name', 'spotify_episode_uri', 'reason_start', 'reason_end', 
                       'shuffle', 'skipped', 'offline', 'offline_timestamp', 'incognito_mode', 'username'
                       ]
        # add surrogate ky stream_id
        generate_surrogate_key(df, source_info_columns)

        with duckdb.connect() as con:
            con.register('raw_spotify_streams_for_dedup', df)
            query = """
                with with_rn as (
                    select 
                    row_number() over (window_by_stream_id) as rn_by_stream_id,
                    count() over (window_by_stream_id) as count_by_stream_id,
                    *
                    from raw_spotify_streams_for_dedup
                    window window_by_stream_id as (PARTITION BY stream_id)
                )
                select * 
                from with_rn 
                where rn_by_stream_id = 1
                order by ts, rn_by_stream_id  
            """
            dedup_by_hash = con.execute(query).fetchdf()

            
        dedup_by_hash['streamed_year'] = dedup_by_hash['ts'].dt.year
        dedup_by_hash.rename(columns={'ts': 'streamed_at'}, inplace=True)

        new_columns = ['stream_id', 'streamed_year' ]
        remove_columns=["rn_by_stream_id", "count_by_stream_id", "snapshot_date"]
        old_columns =  [col for col in dedup_by_hash.columns if col not in (new_columns + remove_columns)]
        return dedup_by_hash[new_columns + old_columns]
        
        print(dedup_by_hash.head(1))
        print(df.head(1))

    def store_to_partition(self, df):
        table = pa.Table.from_pandas(df)
        #destination_full_path= "gs://spotify-insights-pipeline-data/stg_spotify_streams"
        destination_full_path = self.get_destination_full_path()
        print(destination_full_path)
        pq.write_to_dataset(
            table,
            root_path=destination_full_path,
            partition_cols=['username', 'streamed_year']
        )      



    def transform(self):
        self.logger.info(f" {self.source_bucket_name}   {self.source_path} {self.destination_path}")
        source_full_path = self.get_source_full_path()        
        raw_spotify_streams = pd.read_parquet(source_full_path, engine="pyarrow")
        self.normalize_raw_partition_columns(raw_spotify_streams)
        self.transform_offline_timestamp(raw_spotify_streams)        
        self.transform_datatypes_timestamp(raw_spotify_streams)
        raw_spotify_streams = self.deduplicate(raw_spotify_streams)
        self.store_to_partition(raw_spotify_streams)

                    
def main():
    #extract params from args or environment specific vars
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 

    source_bucket_name= os.getenv("SPOTIFY_PIPELINE_BUCKET")
    source_path= os.getenv("SPOTIFY_RAW_PARQUET_RELATIVE_PATH")    
    destination_bucket_name= os.getenv("SPOTIFY_PIPELINE_BUCKET")
    destination_path= os.getenv("SPOTIFY_STG_DTYPE_DEDUP_PARQUET_RELATIVE_PATH")

    transformer = SpotifyTypeDedupTransformer(        
        source_bucket_name=source_bucket_name,
        source_path=source_path,
        destination_bucket_name=destination_bucket_name,
        destination_path=destination_path        
    )
    transformer.transform()

if __name__ == "__main__":
    main()
