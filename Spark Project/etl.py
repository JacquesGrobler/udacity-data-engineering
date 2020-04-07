import os
import configparser
from pyspark.sql import SparkSession

# reads config file
config = configparser.ConfigParser()
config.read('dl.cfg')

# sets environment variable to credentials found in dl.cfg 
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

def create_spark_session():
    """
    Creates Spark Session.
    """
    print("Creating spark session...")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_song_data(spark, dfsongs, destination_bucket):
    """
    1: Creates a temporary view (staging_songs).
    2: From the temporary view uses spark.sql to create songs_table and artists_table.
    3: Writes these created tables to an s3 bucket.
    """
    
    # Creates temporary view (staging_songs)
    print("Creating temporary staging_songs table...")
    dfsongs.createOrReplaceTempView("staging_songs")
    print("Temporary staging_songs table created")
    
    # Creates songs_table, ordered by year and artist, from staging_songs
    print("Creating songs_table table...")
    songs_table = spark.sql("""
                            select distinct
                            cast(s.song_id as String) as song_id,
                            cast(s.title as String) as title,
                            cast(s.artist_id as String) as artist_id,
                            cast(s.year as integer) as year,
                            cast(s.duration as float) as duration
                            from staging_songs as s
                            WHERE s.song_id is not null
                            AND s.title is not null
                            AND s.artist_id is not null
                            AND s.year is not null
                            AND s.duration is not null
                            order by year, artist_id
                            """)
    print("songs_table defined")
    
    # write songs_table to parquet files partitioned by year and artist and loads them into s3 bucket.
    print("Loading songs_table to s3 bucket...")
    songs_table.write.partitionBy("year","artist_id").parquet(destination_bucket+"songs_table.parquet", mode="overwrite")
    print("songs_table successfully loaded")

    # Creates artists_table from staging_songs
    print("Creating songs_table table...")
    artists_table = spark.sql("""
                              select distinct
                              cast(s.artist_id as String) as artist_id,
                              cast(s.artist_name as String) as name,
                              cast(s.artist_location as String) as location,
                              cast(s.artist_latitude as String) as latitude,
                              cast(s.artist_longitude as String) as longitude
                              from staging_songs as s
                              where s.artist_id is not null
                              and s.artist_name is not null
                              order by name
                              """)
    print("artists_table defined")
    
    # write artists_table to parquet files and loads them into s3 bucket.
    print("Loading artists_table to s3 bucket...")
    artists_table.write.parquet(destination_bucket+"artists_table.parquet", mode="overwrite")
    print("artists_table successfully loaded")

def process_log_data(spark, dfevents, dfsongs, destination_bucket):
    """
    1: Creates temporary views (staging_events and staging_songs).
    2: From the temporary view uses spark.sql to create user_table, time_table and songplays_table.
    3: Writes these created tables to an s3 bucket.
    """
    
    # Creates temporary view (staging_events)
    print("Creating temporary staging_events table...")
    dfevents.createOrReplaceTempView("staging_events")
    print("Temporary staging_events table created")

    # Creates user_table from staging_events  
    print("Creating user_table table...")
    user_table = spark.sql("""
                           select distinct
                           cast(e.userid as String) as user_id,
                           cast(e.firstname as String) as first_name,
                           cast(e.lastname as String) as last_name,
                           cast(e.gender as String) as gender,
                           cast(e.level as String) as level
                           from staging_events as e
                           where e.page = 'NextSong'
                           and e.userid is not null
                           and e.firstname is not null
                           and e.lastname is not null
                           and e.gender is not null
                           and e.level is not null
                           order by first_name, last_name
                           """)
    print("user_table defined")
    
    # write user_table to parquet files and loads them into s3 bucket.
    print("Loading user_table to s3 bucket...")
    user_table.write.parquet(destination_bucket+"user_table.parquet", mode="overwrite")
    print("user_table successfully loaded")
    
    # Creates user_table, ordered by year and month, from staging_events 
    print("Creating time_table table...")
    time_table = spark.sql("""
                            select distinct
                            cast(t.ts as timestamp) as ts,
                            cast(extract(hour from t.timestamp) as integer) as hour,
                            cast(extract(day from t.timestamp) as integer) as day,
                            cast(extract(week from t.timestamp) as integer) as week,
                            cast(extract(month from t.timestamp) as integer) as month,
                            cast(extract(year from t.timestamp) as integer) as year,
                            cast(weekday(t.timestamp) as integer) as weekday
                            from (
                            select e.ts, cast(to_timestamp(cast(e.ts/1000 as bigint)) as timestamp) AS timestamp
                            from staging_events as e
                            where e.ts is not null) as t
                            order by year, month
                            """)
    print("time_table defined")
    
    # write user_table to parquet files partitioned by year and month and loads them into s3 bucket.
    print("Loading time_table to s3 bucket...")
    time_table.write.partitionBy("year","month").parquet(destination_bucket+"time_table.parquet", mode="overwrite")
    print("time_table successfully loaded")
    
    # Creates temporary view (staging_songs)
    print("Creating temporary staging_songs table...")
    dfsongs.createOrReplaceTempView("staging_songs")
    print("Temporary staging_songs table created")

    # Creates songplays_table, ordered by start_time, by joining staging_events and staging_songs 
    print("Creating songplays_table table...")
    songplays_table = spark.sql("""
                                select distinct
                                cast(e.ts as timestamp) as start_time,
                                cast(e.userid as String) as user_id,
                                cast(e.level as String) as level,
                                cast(s.song_id as String) as song_id,
                                cast(s.artist_id as String) as artist_id,
                                cast(e.sessionid as String) as session_id,
                                cast(e.location as String) as location,
                                cast(e.useragent as String) as user_agent
                                from staging_events as e
                                left join staging_songs as s on (e.Artist = s.artist_name and e.song = s.title)
                                where e.page = 'NextSong'
                                and e.ts is not null
                                and e.userid is not null
                                and e.level is not null
                                and e.useragent is not null
                                order by start_time
                                """)
    print("songplays_table defined")

    # write songplays table to parquet files partitioned by year and month and loads them to s3 bucket.
    print("Loading songplays_table to s3 bucket...")
    songplays_table.write.partitionBy("start_time").parquet(destination_bucket+"songplays_table.parquet", mode="overwrite")
    print("songplays_table successfully loaded")

def main():
    """
    1: Creates spark session.
    2: Declares song and event file paths, as well as the destination s3 bucket link.
    3: Reads song and event data.
    4: Calls process_song_data and process_log_data functions.
    5: Stops spark session.
    """
    spark = create_spark_session()
    input_song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    input_events_data = "s3a://udacity-dend/log_data/*/*/*.json"
    destination_bucket = "s3a://udacity-spark-bucket/"
    
    print("Reading song data...")
    dfsongs = spark.read.json(input_song_data)
    print("Song data has been read")
    
    print("Reading log data...")
    dfevents = spark.read.json(input_events_data)
    print("Log data has been read")
    
    process_song_data(spark, dfsongs, destination_bucket)    
    process_log_data(spark, dfevents, dfsongs, destination_bucket)
    
    spark.stop()
    print("Spark Stopped.")

if __name__ == "__main__":
    main()
