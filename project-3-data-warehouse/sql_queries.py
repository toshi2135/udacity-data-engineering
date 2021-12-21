import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
DWH_ROLE_ARN = config.get('IAM_ROLE','ARN')
S3_LOG_DATA = config.get('S3','LOG_DATA')
S3_SONG_DATA = config.get('S3','SONG_DATA')
S3_LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    artist varchar, 
    auth varchar, 
    firstName varchar,
    gender varchar, 
    itemInSession int, 
    lastName varchar, 
    length numeric, 
    level varchar, 
    location varchar, 
    method varchar, 
    page varchar, 
    registration numeric, 
    sessionId int, 
    song varchar, 
    status varchar,
    ts bigint, 
    userAgent varchar, 
    userId int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs int,
    artist_id varchar,
    artist_latitude varchar,
    artist_longitude varchar,
    artist_location varchar,
    artist_name varchar,
    song_id varchar,
    title varchar,
    duration numeric,
    year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id int identity(0,1) PRIMARY KEY, 
    start_time timestamp NOT NULL, 
    user_id int NOT NULL, 
    level varchar, 
    song_id varchar, 
    artist_id varchar, 
    session_id int, 
    location varchar, 
    user_agent text
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id int PRIMARY KEY, 
    first_name varchar, 
    last_name varchar, 
    gender varchar, 
    level varchar
    );
    
    ALTER TABLE songplays
    ADD CONSTRAINT fk_users
    FOREIGN KEY (user_id)
    REFERENCES users(user_id);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id varchar PRIMARY KEY, 
    title varchar, 
    artist_id varchar, 
    year int, 
    duration numeric
    );
    
    ALTER TABLE songplays
    ADD CONSTRAINT fk_songs
    FOREIGN KEY (song_id)
    REFERENCES songs(song_id);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar PRIMARY KEY, 
    name varchar, 
    location varchar, 
    latitude varchar, 
    longitude varchar
    );
    
    ALTER TABLE songplays
    ADD CONSTRAINT fk_artists
    FOREIGN KEY (artist_id)
    REFERENCES artists(artist_id);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time timestamp PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month int, 
    year int, 
    weekday int
    );
    
    ALTER TABLE songplays
    ADD CONSTRAINT fk_time
    FOREIGN KEY (start_time)
    REFERENCES time(start_time);
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    format json as {};
""").format(S3_LOG_DATA, DWH_ROLE_ARN, S3_LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    region 'us-west-2';
""").format(S3_SONG_DATA, DWH_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    TRUNCATE TABLE songplays;
    
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT timestamp 'epoch' + cast(staging_events.ts AS bigint)/1000 * interval '1 second' AS epoch_to_timestamp,
    staging_events.userid,
    staging_events.level,
    songs.song_id,
    artists.artist_id,
    staging_events.sessionId,
    artists.location,
    staging_events.userAgent
    FROM staging_events
    LEFT JOIN songs ON staging_events.song = songs.title
        AND staging_events.length = songs.duration
    LEFT JOIN artists ON staging_events.artist = artists.name
    WHERE staging_events.page = 'NextSong'
    ;
""")

user_table_insert = ("""
    TRUNCATE TABLE users;
    
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page = 'NextSong'
    ;
""")

song_table_insert = ("""
    TRUNCATE TABLE songs;
    
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT song_id, title, artist_id, year, duration 
    FROM staging_songs
    ;
""")

artist_table_insert = ("""
    TRUNCATE TABLE artists;
    
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    ;
""")

time_table_insert = ("""
    TRUNCATE TABLE time;

    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    WITH tmp AS (
        SELECT timestamp 'epoch' + cast(ts AS bigint)/1000 * interval '1 second' AS epoch_to_timestamp 
        FROM staging_events
        WHERE page = 'NextSong'
        ) 
    SELECT epoch_to_timestamp AS start_time, 
    EXTRACT(hour from epoch_to_timestamp) AS hour, 
    EXTRACT(day from epoch_to_timestamp) AS day, 
    EXTRACT(week from epoch_to_timestamp) AS week,
    EXTRACT(month from epoch_to_timestamp) AS month,
    EXTRACT(year from epoch_to_timestamp) AS year,
    EXTRACT(weekday from epoch_to_timestamp) AS weekday
    FROM tmp
    ;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
