# Data Warehouse: Project

## Table of Contents
- Intruduction
- Technologies Used
- Instructions to Run the Script
- Summary of each function in create_tables.py
- Summary of each function in etl.py

### Introduction

A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The aim of this project is to create tables on their Redshift database consisting of four dimension tables (song, artist, time, user) and one fact table (songplay) to help the analysts understand user activity.

This project consists of three python scripts. (1) sql_queries.py contains queries to drop tables, create tables, copy data from the s3 bucket to staging tables and insert data into the final tables in AWS Redshift. These queries are stored as variables. (2) create_tables.py connects to the database and uses the sql_queries.py script to drop tables and re-create empty tables. (3) etl.py first copies data from the s3 bucket to the two staging tables, then inserts data to the final tables from the two staging tables and finally deletes any duplicate records in the user and artist tables. 

### Technologies Used

1: AWS S3

2: AWS Redshift

3: Python 3.6.3

### Instructions to Run the Scripts

1: Create an IAM role on AWS and attach the "AmazonS3ReadOnlyAccess" policy to it.

2: Create your AWS cluster and attach the IAM role, you just created, to it.
   Note: Ensure that the region of your cluster is the same as the s3 bucket, for this project it was set to us-west-2.

3: Add the cluster and IAM role information to dwh.cfg file.

4: Open the terminal and enter the following: **python3 create_tables.py**.
   This connects to the database, drops existing tables and create the needed, empty tables.

5: Enter the following in the terminal: **python3 etl.py**.
   This script populates all the tables with relevant data. 
   
6: Go to your Redshift cluster and select the editor. Once you've connected to your database, run the following queries to check that the data has been loaded:
   
   **select * FROM public.artists LIMIT 100;
select * FROM public.songplays LIMIT 100;
select * FROM public.songs LIMIT 100;
select * FROM public.staging_events LIMIT 100;
select * FROM public.staging_songs LIMIT 100;
select * FROM public.time LIMIT 100;
select * FROM public.users LIMIT 100;**

### Summary of each Function in create_tables.py

**drop_tables(cur, conn):**
Connects to the redshift database and drops all the tables if they exist.
'drop_table_queries' can be found in sql_queries.py.

**create_tables(cur, conn):**
Connects to the redshift database and creates all the necessary tables.
'create_table_queries' can be found in sql_queries.py.

**main():**
1. Reads the dwh.cfg file and establishes a connection to the redshift database.
2. Executes the drop_tables and create_tables functions.
3. Closes the connection.

### Summary of each Function in etl.py

**load_staging_tables(cur, conn):**
Connects to the redshift database and copies data from the s3 bucket to the two staging tables.
'copy_table_queries' can be found in sql_queries.py.

**insert_tables(cur, conn):**
Connects to the redshift database and inserts data into the final tables from the two staging tables.
'insert_table_queries' can be found in sql_queries.py.

**delete_duplicates(cur, conn):**
Connects to the redshift database and deletes any duplicate records from the user and aritist tables.
Only the latest records are used.
'delete_table_duplicate_record_queries' can be found in sql_queries.py.

**main():**
1. Reads the dwh.cfg file and establishes a connection to the redshift database.
2. Executes the load_staging_tables, insert_tables and delete_duplicates functions.
3. Closes the connection.




    
