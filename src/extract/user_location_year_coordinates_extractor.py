import pandas as pd
import os
import duckdb
import logging

class UserLocationYearCoordinatesExtractor:
    def __init__(self, airport_location_path: str, user_location_path: str, years_location_path: str):
        """
        """ 
        self.airport_location_path = airport_location_path
        self.user_location_path = user_location_path
        self.years_location_path = years_location_path
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    def extract(self) -> pd.DataFrame:
               
        logging.info(f"Extracting airport location from {self.airport_location_path}")
        airport_location = pd.read_csv(self.airport_location_path, delimiter=";")
        airport_location.rename(columns={'Airport Code': 'airport_code'}, inplace=True)

        logging.info(f"Extracting user location from {self.user_location_path}")
        user_location = pd.read_csv(self.user_location_path)
        user_location['start_at'] = user_location['start_at'].astype("datetime64[ns, UTC]") 
        user_location['end_at'] = user_location['end_at'].astype("datetime64[ns, UTC]") 
        
        con =  duckdb.connect()
        con.register('airport_location', airport_location)
        con.register('user_location', user_location)

        query = """
            with recursive user_location_min_max as (
                select 
                    extract('year' FROM min(start_at))  as min_year,
                    extract('year' FROM max(end_at))  as max_year
                from user_location
            ),
            years_list as (
                select min_year as year
                from user_location_min_max
                union all 
                select year + 1
                from years_list
                join user_location_min_max on year < max_year
            ),
            years_code as (
                select distinct y.year, u.airport_code
                from years_list y
                join user_location u 
                    on y.year between 
                        extract('year' FROM u.start_at) 
                        and extract('year' FROM u.end_at)         
            ),
            years_coordinates as (
                select yc.year, yc.airport_code, a.Latitude, a.Longitude
                from years_code yc 
                join airport_location a on a.airport_code = yc.airport_code
            )
            select year, airport_code, Latitude as latitude, Longitude as longitude
            from years_coordinates
            order by year, Latitude
            """        
        years_coordinates = con.execute(query).fetchdf()    
        years_coordinates.to_csv(self.years_location_path, index=False)
    
def main():   
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv()) 

    airport_location_path = f"gs://{os.getenv('AIRPORT_CODE_LOCATION_DESTINATION_BUCKET')}/{os.getenv('AIRPORT_CODE_LOCATION_DESTINATION_PATH')}"
    user_location_path = f"gs://{os.getenv('USER_LOCATION_DESTINATION_BUCKET')}/{os.getenv('USER_LOCATION_DESTINATION_PATH')}"
    years_location_path = f"gs://{os.getenv('YEARS_LOCATION_DESTINATION_BUCKET')}/{os.getenv('YEARS_LOCATION_LOCATION_DESTINATION_PATH')}"

    extractor = UserLocationYearCoordinatesExtractor(airport_location_path, user_location_path, years_location_path)
    extractor.extract()
    
if __name__ == "__main__":
    main()