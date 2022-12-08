import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config.get("IAM_ROLE", "ARN")
LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        event_id        BIGINT IDENTITY(0, 1),
        artist          VARCHAR,
        auth            VARCHAR,
        firstName       VARCHAR(80),
        gender          CHAR(1),
        itemInSession   INT,
        lastName        VARCHAR(80),
        length          FLOAT,
        level           VARCHAR(10),
        location        VARCHAR(500),
        method          VARCHAR,
        page            VARCHAR,
        registration    VARCHAR,
        sessionId       INT SORTKEY DISTKEY,
        song            VARCHAR,
        status          INT,
        ts              BIGINT,
        userAgent       VARCHAR(500),
        userId          INT
    )
""")  # Use sessionId as sortkey and distkey

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs           INT,
        artist_id           VARCHAR(50) SORTKEY DISTKEY,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR(500),
        artist_name         VARCHAR,
        song_id             VARCHAR(40),
        title               VARCHAR,
        duration            FLOAT,
        year                INT
    )
""")  # Use artist_id as sortkey and distkey

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
        songplay_id     INT IDENTITY(0, 1) NOT NULL SORTKEY,
        start_time      TIMESTAMP NOT NULL,
        user_id         INT NOT NULL DISTKEY,
        level           VARCHAR(10) NOT NULL,
        song_id         VARCHAR(40) NOT NULL,
        artist_id       VARCHAR(50) NOT NULL,
        session_id      INT NOT NULL,
        location        VARCHAR(500),
        user_agent      VARCHAR(500)
    )
""")  # Use songplay_id as sortkey and user_id as distkey

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id         INT NOT NULL SORTKEY,
        first_name      VARCHAR(80),
        last_name       VARCHAR(80),
        gender          CHAR(1),
        level           VARCHAR(10)
    ) diststyle all
""")  # Use user_id as sortkey and all distribution style

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id     VARCHAR(40) NOT NULL SORTKEY,
        title       VARCHAR NOT NULL,
        artist_id   VARCHAR(50) NOT NULL,
        year        INT NOT NULL,
        duration    FLOAT NOT NULL
    )
""")  # Use song_id as sortkey

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id   VARCHAR(50) NOT NULL SORTKEY,
        name        VARCHAR,
        location    VARCHAR(500),
        latitude    FLOAT,
        longitude   FLOAT
    ) diststyle all
""")  # Use artist_id as sortkey and all distribution style

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time  TIMESTAMP NOT NULL SORTKEY,
        hour        SMALLINT,
        day         SMALLINT,
        week        SMALLINT,
        month       SMALLINT,
        year        SMALLINT,
        weekday     VARCHAR
    ) diststyle all
""")  # Use start_time as sortkey and all distribution style

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {}
    credentials 'aws_iam_role={}'
    format as json {}
    STATUPDATE ON
    region 'us-west-2';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs FROM {}
    credentials 'aws_iam_role={}'
    format as json 'auto'
    STATUPDATE ON
    region 'us-west-2';
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT  DISTINCT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second' AS start_time,
            se.userId, se.level, ss.song_id, ss.artist_id, se.sessionId, se.location, se.userAgent
    FROM staging_events AS se
    INNER JOIN staging_songs AS ss
    ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId, firstName, lastName, gender, level
    FROM staging_events
    WHERE page = 'NextSong' AND userId IS NOT NULL
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration
    FROM staging_songs
    WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT start_time, 
           EXTRACT(hour FROM start_time), 
           EXTRACT(day FROM start_time), 
           EXTRACT(week FROM start_time), 
           EXTRACT(month FROM start_time), 
           EXTRACT(year FROM start_time), 
           EXTRACT(weekday FROM start_time)
    FROM songplays
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create,
                        songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop,
                      songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert,
                        song_table_insert, artist_table_insert, time_table_insert]
