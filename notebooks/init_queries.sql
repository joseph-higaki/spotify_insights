CREATE OR REPLACE EXTERNAL TABLE spotify-insights-444509.raw_spotify_stream
OPTIONS( 
    format = 'parquet', 
    uris = ['gs://spotify-insights-pipeline-data/=raw-tables']
);


ts	
platform
ms_played
conn_country
ip_addr
master_metadata_track_name
master_metadata_album_artist_name
master_metadata_album_album_name
spotify_track_uri
episode_name
episode_show_name
spotify_episode_uri
reason_start
reason_end
shuffle
skipped
offline
offline_timestamp
incognito_mode
user_name
snapshot_date
platform
conn_country
master_metadata_track_name
master_metadata_album_artist_name
master_metadata_album_album_name
reason_start
reason_end