import configparser
import psycopg2

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
#DWH_ROLE_ARN = config.get('IAM_ROLE', 'ARN')
# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES
#(songplay_id,start_time,user_id,level,
#song_id,artist_id,session_id,location,user_agent)

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events_table(
    artist VARCHAR(255),
    auth VARCHAR(50) NOT NULL,
    firstName VARCHAR(255),
    gender CHAR(1),
    itemInSession INT,
    lastName VARCHAR(255),
    length FLOAT,
    level VARCHAR(50) NOT NULL,
    location VARCHAR(255),
    method VARCHAR(10) NOT NULL,
    page VARCHAR(50) NOT NULL,
    registration FLOAT,
    sessionId INT NOT NULL,
    song VARCHAR(255),
    status INT,
    ts BIGINT NOT NULL,
    userAgent VARCHAR(255),
    userId VARCHAR(10) NOT NULL
);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs_table(
    song_id VARCHAR(18) NOT NULL,
    title VARCHAR(255) NOT NULL,
    duration FLOAT,
    num_songs INT,
    year INT,
    artist_id VARCHAR(18) NOT NULL,
    artist_name VARCHAR(255) NOT NULL,
    artist_location VARCHAR(255),
    artist_latitude FLOAT,
    artist_longitude FLOAT
)
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay_table (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY DISTKEY SORTKEY,
    start_time TIMESTAMP,
    user_id VARCHAR(10) NOT NULL,
    level VARCHAR(50),
    song_id VARCHAR(18),
    artist_id VARCHAR(18),
    session_id INT,
    location VARCHAR(255),
    user_agent VARCHAR(255)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS user_table (
    user_id VARCHAR(10) PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    gender CHAR(1),
    level VARCHAR(50)
)
DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song_table(
    song_id VARCHAR(18) PRIMARY KEY,
    title VARCHAR(255),
    artist_id VARCHAR(18),
    year INT,
    duration FLOAT
)
DISTSTYLE ALL;
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist_table(
    artist_id VARCHAR(18) PRIMARY KEY,
    name VARCHAR(255),
    location VARCHAR(255),
    latitude FLOAT,
    longitude FLOAT
)
DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time_table(
    start_time TIMESTAMP PRIMARY KEY DISTKEY SORTKEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events_table from 's3://udacity-dend/log_data' 
IAM_ROLE {}
FORMAT AS JSON 's3://udacity-dend/log_json_path.json'
region 'us-west-2';
""").format(*config['IAM_ROLE'].values())
 
staging_songs_copy = ("""
copy staging_songs_table from 's3://udacity-dend/song_data' 
IAM_ROLE {}
FORMAT AS JSON 'auto'
region 'us-west-2';
""").format(*config['IAM_ROLE'].values())

# FINAL TABLES


user_table_insert = ("""
INSERT INTO user_table(user_id, first_name, last_name, gender, level)
SELECT DISTINCT
    userId AS user_id,
    firstName AS first_name,
    lastName AS last_name,
    gender,
    level
FROM staging_events_table
WHERE page = 'NextSong';
""")

artist_table_insert = ("""
INSERT INTO artist_table(artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs_table;
""")

song_table_insert = ("""
INSERT INTO song_table(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs_table;
""") 


time_table_insert = ("""
INSERT INTO time_table(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time,
    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(WEEK FROM start_time) AS week,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    EXTRACT(DOW FROM start_time) AS weekday
FROM staging_events_table;
""")

songplay_table_insert = ("""
INSERT INTO songplay_table(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT 
    timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,
    se.userId AS user_id,
    se.level,
    ss.song_id,
    ss.artist_id,
    se.sessionId AS session_id,
    se.location,
    se.userAgent AS user_agent
FROM staging_events_table se
JOIN staging_songs_table ss ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration 
WHERE se.page = 'NextSong';
""")
                         
most_song_query = ("""SELECT s.song_id, s.title, COUNT(*) AS play_count
FROM songplay_table sp
JOIN song_table s ON sp.song_id = s.song_id
GROUP BY s.song_id, s.title
ORDER BY play_count DESC
LIMIT 5;""")
                   
highest_usage_time_of_day = ("""
SELECT EXTRACT(HOUR FROM start_time) AS hour_of_day, COUNT(*) AS play_count
FROM songplay_table
GROUP BY hour_of_day
ORDER BY play_count DESC
LIMIT 5;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
query_tables_queries = [most_song_query,highest_usage_time_of_day]
