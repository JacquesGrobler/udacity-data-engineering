# Data Lake: Project

## Table of Contents
- Intruduction
- Summary of each function in etl.py
- Technologies Used
- Instructions to Run the Script

### Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The aim of this project is to load data from their s3 bucket, create tables: four dimension tables (song, artist, time, user) and one fact table (songplay) and then load these tables back to a s3 bucket.

This project consists of one python scripts: etl.py. This script contains 4 functions which accomplishes the aim of this project, namely: create_spark_session(), process_song_data(spark, dfsongs), process_log_data(spark, dfevents, dfsongs) and main(). The explanation of each function now follows.

### Summary of each Function in etl.py

**create_spark_session():**
Creates a spark session.

**process_song_data(spark, dfsongs):**
1. Creates a temporary view (staging_songs).
2. From the temporary view uses spark.sql to create songs_table and artists_table.
3. Writes these created tables to an s3 bucket.

**process_log_data(spark, dfevents, dfsongs):**
1. Creates temporary views (staging_events and staging_songs).
2. From the temporary view uses spark.sql to create user_table, time_table and songplays_table.
3. Writes these created tables to an s3 bucket.

**main():**
1. Creates spark session.
2. Declares song and event file paths, as well as the destination s3 bucket link.
3. Reads song and event data.
4. Calls process_song_data and process_log_data functions.
5. Stops spark session.

### Technologies Used

- AWS S3
- Python 3.6.3
- PySpark

### Instructions to Run the Scripts

1. Create a user on AWS with Admin access, if you don't have one already.
2. Enter the ID and Secret of the user in the dl.cfg file.
3. On line 186 in the etl.py file, replace the s3 bucket destination (s3a://udacity-spark-bucket/) to your desired choice.
4. Open terminal and run this command: *python3 etl.py*.