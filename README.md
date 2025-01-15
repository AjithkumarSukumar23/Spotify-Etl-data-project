# Spotify ETL Pipeline

## Overview
This project demonstrates a robust ETL (Extract, Transform, Load) pipeline designed to process Spotify playlist data efficiently. Using a combination of AWS services, Snowflake, and Power BI, the pipeline extracts raw data from Spotify's API, transforms it into a structured format, and loads it into Snowflake for analysis and visualization. The pipeline is fully automated, scalable, and provides near-real-time updates and actionable insights.

---

## Project

### 1. **Data Extraction: AWS Lambda**
- **Spotify API Integration:**  
  Raw playlist data in JSON format is extracted using the Spotify API, with client credentials (Client ID and Secret) securely stored in Lambda environment variables.
- **Automation with CloudWatch:**  
  The `lambda_function.py` script performs the following tasks:
  - Retrieves playlist details using the Spotify API.
  - Saves raw JSON data into an S3 bucket (`spotify-data-manvith`) under the folder `raw_data/to-be-processed_data/`, naming files with timestamps for unique identification.
  - Triggers the **AWS Glue Job** (`Spotify_transformation_job`) to process the data.

---

### 2. **Data Transformation: AWS Glue**
- **PySpark-Based Transformation:**  
  The `spark_transformation.py` script processes raw JSON data to extract details about albums, artists, and songs.
- **Steps Performed:**
  - **Data Processing:**
    - `process_album`: Extracts album details such as ID, name, release date, total tracks, and URL.
    - `process_artists`: Extracts artist details such as ID, name, and URL.
    - `process_songs`: Extracts song details such as ID, name, duration, popularity, and links to corresponding albums and artists.
  - **Transformations:**
    - Flattening nested JSON structures using PySpark’s `explode` function.
    - Formatting the `added_at` field to a standard DATE format.
- **Output:**  
  Transformed data is saved in S3 under the `transformed_data/` folder, categorized into subfolders (`album`, `artists`, `songs`) as CSV files.

---

### 3. **Data Loading: Snowflake**
- **Database Setup:**  
  A Snowflake database (`spotify_db`) is created with tables:
  - `tbl_album`: Stores album data.
  - `tbl_artists`: Stores artist data.
  - `tbl_songs`: Stores song data.
- **Snowpipe Integration:**
  - An S3 bucket (`spotify-project-ajith`) is integrated with Snowflake using **Storage Integration** (`s3_init`).
  - Snowpipe automatically ingests transformed data into the respective Snowflake tables for near-real-time updates using event notification trigger.

---

### 4. **Data Visualization: Power BI**
- **Snowflake Integration:**  
  Snowflake tables are connected to Power BI to create interactive dashboards.
- **Key Visualizations:**
  - Top 10 artists with the highest song popularity.
  - Songs popularity by album.
  - Song duration summaries.
  - Total tracks by each artist.

---

## Features
- **Automation:**  
  Fully automated ETL pipeline using CloudWatch, AWS Glue, and Snowpipe.
- **Scalability:**  
  AWS Lambda and Glue efficiently handle large volumes of data.
- **Real-Time Updates:**  
  Snowpipe ensures near-real-time ingestion of transformed data into Snowflake.
- **Customizable Transformations:**  
  PySpark-based transformations can adapt to varying data structures or additional requirements.

---

## Tools and Technologies
- **Cloud Services:** AWS Lambda, Amazon CloudWatch, Amazon S3, AWS Glue, Snowflake, Snowpipe.
- **Programming Languages:** Python, SQL, PySpark.
- **File Formats:** JSON, CSV.
- **Visualization Tools:** Power BI.
- **Security:** AWS IAM Roles, AWS Storage Integration.



