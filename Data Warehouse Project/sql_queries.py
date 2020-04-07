import configparser


# CONFIG

config = configparser.ConfigParser()
config.read('dwh.cfg')
LOG_DATA=config.get('S3','LOG_DATA')
SONG_DATA= config.get('S3','SONG_DATA')
ARN= config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create = """CREATE TABLE staging_events
                                 (artist VARCHAR, 
                                  auth VARCHAR, 
                                  firstName VARCHAR, 
                                  gender VARCHAR, 
                                  itemInSession INTEGER, 
                                  lastName VARCHAR, 
                                  length NUMERIC, 
                                  level VARCHAR, 
                                  location VARCHAR, 
                                  method VARCHAR, 
                                  page VARCHAR, 
                                  registration VARCHAR, 
                                  sessionId NUMERIC, 
                                  song VARCHAR, 
                                  status NUMERIC, 
                                  ts NUMERIC, 
                                  userAgent INTEGER, 
                                  userId INTEGER);
"""

staging_songs_table_create = """CREATE TABLE staging_songs 
                                (num_songs INTEGER, 
                                 artist_id VARCHAR,
                                 artist_latitude VARCHAR, 
                                 artist_longitude VARCHAR,
                                 artist_location VARCHAR,
                                 artist_name VARCHAR,
                                 song_id VARCHAR, 
                                 title VARCHAR, 
                                 duration NUMERIC,
                                 year INTEGER);
"""

songplay_table_create = """CREATE TABLE songplays 
                            (songplay_id INT IDENTITY(1,1), 
                             start_time NUMERIC NOT NULL SORTKEY, 
                             user_id NUMERIC NOT NULL, 
                             level VARCHAR NOT NULL, 
                             song_id VARCHAR, 
                             artist_id VARCHAR, 
                             session_id NUMERIC, 
                             location VARCHAR, 
                             user_agent VARCHAR NOT NULL)
                             diststyle auto;
"""

user_table_create = """CREATE TABLE users 
                        (record_number INT IDENTITY(1,1),
                         user_id INTEGER PRIMARY KEY, 
                         first_name VARCHAR NOT NULL, 
                         last_name VARCHAR NOT NULL, 
                         gender VARCHAR NOT NULL, 
                         level VARCHAR NOT NULL)
                         SORTKEY(first_name, last_name);
"""

song_table_create = """CREATE TABLE songs 
                        (record_number INT IDENTITY(1,1),
                         song_id VARCHAR PRIMARY KEY, 
                         title VARCHAR NOT NULL SORTKEY, 
                         artist_id VARCHAR NOT NULL, 
                         year INTEGER NOT NULL, 
                         duration NUMERIC NOT NULL)
                         diststyle auto;
"""

artist_table_create = """CREATE TABLE artists 
                          (record_number INT IDENTITY(1,1),
                           artist_id VARCHAR PRIMARY KEY, 
                           name VARCHAR NOT NULL SORTKEY, 
                           location VARCHAR, 
                           latitude VARCHAR, 
                           longitude VARCHAR)
                           diststyle auto;
"""

time_table_create = """CREATE TABLE time 
                        (record_number INT IDENTITY(1,1),
                         start_time BIGINT PRIMARY KEY SORTKEY, 
                         hour INTEGER NOT NULL, 
                         day INTEGER NOT NULL, 
                         week INTEGER NOT NULL, 
                         month INTEGER NOT NULL, 
                         year INTEGER NOT NULL, 
                         weekday INTEGER NOT NULL)
                         diststyle auto;
"""

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    format as json 'auto' compupdate off region 'us-west-2';
""").format(LOG_DATA, ARN)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto' compupdate off region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
select distinct
 e.ts as start_time,
 e.userid as user_id,
 e.level,
 s.song_id,
 s.artist_id,
 e.sessionid as session_id,
 e.location,
 e.useragent as user_agent
 from public.staging_events as e
left join public.staging_songs as s on (e.Artist = s.artist_name and e.song = s.title)
where e.page = 'NextSong'
and e.ts is not null
and e.userid is not null
and e.level is not null
and e.useragent is not null;
""")

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level)
select distinct
 e.userid as user_id,
 e.firstname as first_name,
 e.lastname as last_name,
 e.gender,
 e.level
 from public.staging_events as e
where e.page = 'NextSong'
and e.userid is not null
and e.firstname is not null
and e.lastname is not null
and e.gender is not null
and e.level is not null;
""")

song_table_insert = ("""INSERT INTO songs (song_id, title, artist_id, year, duration)
select distinct
s.song_id,
s.title,
s.artist_id,
s.year,
s.duration
from public.staging_songs as s
WHERE s.song_id is not null
AND s.title is not null
AND s.artist_id is not null
AND s.year is not null
AND s.duration is not null;
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
select distinct
s.artist_id,
s.artist_name as name,
s.artist_location as location,
s.artist_latitude as latitude,
s.artist_longitude as longitude
from public.staging_songs as s
where s.artist_id is not null
and s.artist_name is not null;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
select distinct
t.ts,
date_part('hour', t.timestamp) as hour, 
date_part('day', t.timestamp) as day, 
date_part('week', t.timestamp) as week, 
date_part('month', t.timestamp) as month, 
date_part('year', t.timestamp) as year, 
date_part('weekday', t.timestamp) as weekday
from(
select e.ts, cast(timestamp 'epoch' + e.ts/1000 * interval '1 second' as datetime) AS timestamp
from public.staging_events as e
where e.ts is not null) as t;
""")

# DELETE DUPLICATES

songplay_table_delete_duplicates = ("""delete from songplays where songplay_id < 
                                    (select max(songplay_id) from songplays d
                                     where songplays.start_time = d.start_time
                                     and songplays.user_id = d.user_id
                                     and songplays.level = d.level
                                     and songplays.user_agent = d.user_agent);
""")

user_table_delete_duplicates = ("""delete from users where record_number < 
                                    (select max(record_number) from users d
                                     where users.user_id = d.user_id);
""")

song_table_delete_duplicates = ("""delete from songs where record_number < 
                                    (select max(record_number) from songs d
                                     where songs.song_id = d.song_id);
""")

artist_table_delete_duplicates = ("""delete from artists where record_number < 
                                    (select max(record_number) from artists d
                                     where artists.artist_id = d.artist_id
                                     and artists.name = d.name);
""")

time_table_delete_duplicates = ("""delete from time where record_number < 
                                    (select max(record_number) from time d
                                     where time.start_time = d.start_time);
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
delete_table_duplicate_record_queries = [songplay_table_delete_duplicates,  user_table_delete_duplicates, song_table_delete_duplicates,  artist_table_delete_duplicates, time_table_delete_duplicates]
