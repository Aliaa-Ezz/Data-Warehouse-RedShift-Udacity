import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get('IAM_ROLE', 'ARN')
LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSONPATH = config.get('S3', 'LOG_JSONPATH')
SONG_DATA = config.get('S3', 'SONG_DATA')

# DROP TABLES
#this will make sure to drop the tables each time we run create_tables.py

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"
user_table_drop = "DROP TABLE IF EXISTS users"

# we are creating two staging tables for the old schema to 

# Staging tables
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
                event_id        BIGINT IDENTITY(0,1)  NOT NULL,
                artist          VARCHAR               NULL,
                auth            VARCHAR               NULL,
                firstName       VARCHAR               NULL,
                gender          VARCHAR               NULL,
                itemInSession   VARCHAR               NULL,
                lastName        VARCHAR               NULL,
                length          VARCHAR               NULL,
                level           VARCHAR               NULL,
                location        VARCHAR               NULL,
                method          VARCHAR               NULL,
                page            VARCHAR               NULL,
                registration    VARCHAR               NULL,
                sessionId       INTEGER               NOT NULL,
                song            VARCHAR               NULL,
                status          INTEGER               NULL,
                ts              BIGINT                NOT NULL,
                userAgent       VARCHAR               NULL,
                userId          INTEGER               NULL
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
                num_songs            INTEGER           NULL,
                artist_id            VARCHAR           NOT NULL,
                artist_latitude      VARCHAR           NULL,
                artist_longitude     VARCHAR           NULL,
                artist_location      VARCHAR(350)      NULL,
                artist_name          VARCHAR(350)      NULL,
                song_id              VARCHAR           NOT NULL,
                title                VARCHAR(350)      NULL,
                duration             DECIMAL(10)        NULL,
                year                 INTEGER           NULL
    );
""")


#then we will create our dimensions and fact tables

#For the fact table
songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
                songplay_id   INTEGER IDENTITY(0,1)   NOT NULL,
                start_time    TIMESTAMP               NOT NULL,
                user_id       VARCHAR(50)             NOT NULL,
                level         VARCHAR(10)             NOT NULL,
                song_id       VARCHAR(40)             NOT NULL,
                artist_id     VARCHAR(50)             NOT NULL,
                session_id    VARCHAR(50)             NOT NULL,
                location      VARCHAR(100)            NULL,
                user_agent    VARCHAR(255)            NULL
    );
""")

#dimenesion tables
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
                user_id       INTEGER                 NOT NULL,
                first_name    VARCHAR(80)             NULL,
                last_name     VARCHAR(80)             NULL,
                gender        VARCHAR(5)             NULL,
                level         VARCHAR(10)             NULL
    ) diststyle all;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
                song_id       VARCHAR(50)             NOT NULL,
                title         VARCHAR(350)            NOT NULL,
                artist_id     VARCHAR(50)             NOT NULL,
                year          INTEGER                 NOT NULL,
                duration      DECIMAL(10)              NOT NULL
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
                artist_id     VARCHAR(50)              NOT NULL SORTKEY,
                name          VARCHAR(500)             NULL,
                location      VARCHAR(500)             NULL,
                latitude      DECIMAL(10)               NULL,
                longitude     DECIMAL(10)               NULL
    ) diststyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
                start_time    TIMESTAMP               NOT NULL,
                hour          INTEGER                NULL,
                day           INTEGER                NULL,
                week          INTEGER                NULL,
                month         INTEGER                NULL,
                year          INTEGER                NULL,
                weekday       INTEGER                NULL
    ) diststyle all;
""")

#here we will copy the data from the buckets to the staging tables

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)



#inserting the data from the staging tables to our star schema tables

songplay_table_insert = ("""
    INSERT INTO songplays (start_time,user_id,level,song_id,artist_id,session_id,location, user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'   AS start_time,
            se.userId,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionId,
            se.location,
            se.userAgent
    FROM staging_events se
    JOIN staging_songs ss ON se.artist = ss.artist_name
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name,last_name,gender,level)
    SELECT  DISTINCT se.userId,
            se.firstName,
            se.lastName,
            se.gender,
            se.level    
            FROM staging_events se
    WHERE se.page = 'NextSong';
""")

song_table_insert = ("""
    INSERT INTO songs (song_id,title,artist_id,year,duration)
    SELECT  DISTINCT ss.song_id,
            ss.title,
            ss.artist_id,
            ss.year,
            ss.duration
    FROM staging_songs ss;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location,latitude,longitude)
    SELECT  DISTINCT ss.artist_id,
            ss.artist_name,
            ss.artist_location,
            ss.artist_latitude,
            ss.artist_longitude
    FROM staging_songs ss;
""")

time_table_insert = ("""
    INSERT INTO time (start_time,hour,day,  week, month,year,weekday)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts * INTERVAL '1 second'  AS start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(week FROM start_time)    
            FROM    staging_events se
    WHERE se.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]