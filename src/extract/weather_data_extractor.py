import requests
import pandas as pd
import logging
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import argparse
import os 



# src/extract/weather_data_extractor.py
class WeatherDataExtractor:
    def __init__(self, year, latitude, longitude, destination_bucket_name, destination_path):
        """
        Initialize Weather Data Extraction process
        
        Args:
            year (int): Year of the weather data
            latitude (float): Latitude
            longitude (float): Longitude
            destination_bucket_name (str): Bucket for storing raw data
            destination_path (str): path within the Bucket for storing raw data
        """
        self.year = year
        self.latitude = latitude
        self.longitude = longitude
        self.destination_bucket_name = destination_bucket_name
        self.destination_path = destination_path
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def extract_weather_data(self):
        """ Fetch weather data from the Open-Meteo API for a given year and location      
            Store the data in a Google Cloud Storage bucket 
        """          
        start_date = datetime(self.year, 1, 1)
        end_date = datetime(self.year, 12, 31)

        # Define the API URL
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),  
            "hourly": "temperature_2m,cloud_cover",
            "daily": "weather_code,sunrise,sunset,daylight_duration,sunshine_duration",
        }

        # Make the GET request
        logging.info(f"Fetching weather data for {self.year} at {self.latitude}, {self.longitude}")
        response = requests.get(url, params=params)
        response.raise_for_status()  # Raise an error if the request fails

        # Parse the JSON response
        data = response.json()

        # Extract daily data Create a DataFrame
        daily_df = pd.DataFrame(data["daily"])
        # Convert the "time" column to datetime with UTC timezone
        daily_df["time"] = pd.to_datetime(daily_df["time"], format="%Y-%m-%d").dt.tz_localize("UTC")
        # Convert numerical columns to float
        daily_numerical_columns = ["weather_code", "daylight_duration", "sunshine_duration"]
        daily_df[daily_numerical_columns] = daily_df[daily_numerical_columns].astype(float)
        # Rename columns for clarity
        daily_df.rename(columns={"time": "date"}, inplace=True)
        daily_df["year"] = self.year
        daily_df["latitude"] = self.latitude
        daily_df["longitude"] = self.longitude 
        daily_df["frequency"] = "daily"

        # Extract hourly data  DataFrame
        hourly_df = pd.DataFrame(data["hourly"])
        # Convert the "time" column to datetime with UTC timezone
        hourly_df["time"] = pd.to_datetime(hourly_df["time"], format="%Y-%m-%dT%H:%M").dt.tz_localize("UTC")
        # Convert numerical columns to float
        hourly_numerical_columns = ["temperature_2m", "cloud_cover"]
        hourly_df[hourly_numerical_columns] = hourly_df[hourly_numerical_columns].astype(float)
        # Rename columns for clarity
        hourly_df.rename(columns={"time": "datetime"}, inplace=True)
        hourly_df["year"] = self.year
        hourly_df["latitude"] = self.latitude
        hourly_df["longitude"] = self.longitude
        hourly_df["frequency"] = "hourly"
        
        logging.info(f"Extracted {len(daily_df)} daily records")
        self.store_to_partition(daily_df, ['frequency', 'year', 'latitude', 'longitude'])
        logging.info(f"Extracted {len(hourly_df)} hourly records")
        self.store_to_partition(hourly_df, ['frequency', 'year', 'latitude', 'longitude'])

    def get_full_path(self) -> str:
        return f"gs://{self.destination_bucket_name }/{self.destination_path}"

    def store_to_partition(self, df, partition_columns):
            table = pa.Table.from_pandas(df)
            destination_full_path = self.get_full_path()
            logging.info(f"Storing to partition in path: {destination_full_path}")
            pq.write_to_dataset(
                table,
                root_path=destination_full_path,
                partition_cols=partition_columns
            )      

if __name__ == "__main__":
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 

    parser = argparse.ArgumentParser(description="Extract weather data")
    parser.add_argument("--year", type=int, help="Year of the weather data")
    parser.add_argument("--latitude", type=lambda x: round(float(x), 3), help="Latitude (rounded to 3 decimal places)")     
    parser.add_argument("--longitude", type=lambda x: round(float(x), 3), help="Longitude (rounded to 3 decimal places)")
    parser.add_argument("--destination_bucket_name", type=str, help="Bucket for storing raw data")
    parser.add_argument("--destination_path", type=str, help="path within the Bucket for storing raw data")
        
    args = parser.parse_args()
    extractor = WeatherDataExtractor(args.year, args.latitude, args.longitude, args.destination_bucket_name, args.destination_path)
    extractor.extract_weather_data()

    '''
    python src/extract/weather_data_extractor.py --year 2023 --latitude 41.300000 --longitude 2.083333 --destination_bucket_name spotify-insights-pipeline-data --destination_path weather_location
    
    '''