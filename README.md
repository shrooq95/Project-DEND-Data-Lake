# Data Lake

## About the project: 

This Project handles data of a music streaming startup, Sparkify. Data set is a set of files in JSON format stored in AWS S3 buckets and contains two parts: log_data and song_data

## Description: 

Project builds an ETL pipeline to Extract data from JSON files stored in AWS S3, process the data with Apache Spark, and write the data back to AWS S3 as Spark parquet files. 

### It contains the following components:

etl.py: reads data from S3, processes it into analytics tables, and then writes them to S3
dl.cfg: contains  AWS credentials

#### So, transforms data to create five different tables :

#### Fact Table:

songplays - records in log data associated with song plays (records with page NextSong)
            songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
            
#### Dimension Tables:

users - users in the app Fields
        user_id, first_name, last_name, gender, level

songs - songs in music database Fields
        song_id, title, artist_id, year, duration

artists - artists in music database Fields
          artist_id, name, location, lattitude, longitude

time - timestamps of records in songplays broken down into specific units Fields
       start_time, hour, day, week, month, year, weekday
