# Project: Data Warehouse

## Description
Building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables
## Steps
- Connect to the PostgreSQL database using the provided credentials (database name, username, and password).
- Create the necessary tables in the database using SQL CREATE TABLE statements. The program creates the following tables: staging_events, staging_songs, songplays, users, songs, artists, and time. Each table is designed to store specific data related to the music streaming application.
- Load the data from the provided S3 buckets into the staging tables using the COPY command. The program uses the COPY command to load data from the S3 buckets s3://udacity-dend/log_data and s3://udacity-dend/song_data into the staging_events and staging_songs tables, respectively.
- Insert the data from the staging tables into the appropriate dimension tables (users, songs, artists, and time) and the fact table (songplays).
- Execute a SQL query to determine the most played song from the songplays table and the highest usage time of day by hour for songs from the songplays table
- Close the database cursor and connection to release the resources.



## Tech

Dillinger uses a number of open source projects to work properly:

- python 3.11
- AWS S3
- AWS redshift