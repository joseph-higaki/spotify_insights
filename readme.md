# Spotify Insights Pipeline

## Problem Statement
Spotify stores streaming data, even though yearly Warped is a nice visualizition, I'd like to extract other aspects.

## Goals
2. Transform the raw data into structured formats for analysis.
3. Conduct Exploratory Data Analysis (EDA) to uncover data quality issues and guide transformations.
4. Build a dimensional model for in-depth insights.
5. Create a personal music consumption report with visualizations.

## Stages Description

| **Pipeline Stage**       | **Description**                                                                                  | **Source**                                   | **Medallion Layer** | **Destination**                        | **Tool**                                  |
|---------------------------|--------------------------------------------------------------------------------------------------|---------------------------------------------|---------------------|----------------------------------------|------------------------------------------|
| **Extract & Load** | Incrementally upload JSON files to Google Cloud Storage.                                        | User-deposited JSON files at file share    | Bronze              | JSON files in GCS bucket              | Python script orchestrated by Airflow    |
| **Transformations**      | Convert JSON to Parquet for efficient processing.                                               | JSON files in GCS bucket                   | Bronze              | Parquet files in GCS bucket           | Python script orchestrated by Airflow    |
| **EDA**                  | Explore data for patterns, anomalies, and insights.                                             | Parquet files in GCS bucket                | Silver              | EDA notebook or intermediate tables    | Python (Pandas, Matplotlib, Seaborn)     |
| **Analytics Engineering**                  | Deduplicate, cleanse, fix data      | Parquet files in GCS bucket                | Silver              | Cleaned and structured Parquet files  | Python Pandas 
| **Analytics Engineering**                  | Build staging streams table      | Cleaned and structured Parquet files                | Silver              | Big Query Staging      | dbt SQL
| **Analytics Engineering**             | Generate features, and create a dimensional model.                        | Big Query Staging       | Gold         | Big Query Dimensional model   | dbt SQL                              |
| **Visualization**        | Deliver a polished report                                    | Big Query Dimensional model          | Gold                | Interactive dashboard | Looker Studio|
| **Visualization**        | Deliver a polished report                                    | Big Query Dimensional model          | Gold                | Graph Snapshot | Python Flask / Django |



## Pipeline Overview 

![alt text](_resources/pipeline_overview_diagram.md/image.png)

## Future Improvements
- Integrate weather and timezone data.
- Extend to real-time streaming.
- Optimize API usage for external features.
