# Project: Build A Cloud Data Warehouse

This project builds an **ELT pipeline** that extracts data from **S3**, stages them in **Redshift**, and transforms data into a set of **dimensional tables** for Sparkify analytics team to continue finding insights in what songs their users are listening to.


## Project Structure

```
Cloud Data Warehouse
|____create_tables.py    # database/table creation script 
|____etl.py              # ELT builder
|____sql_queries.py      # SQL query collections
|____dwh.cfg             # AWS configuration file
|____test.ipynb          # testing
```


## ELT Pipeline
### [etl.py](etl.py)
ELT pipeline builder

1. `load_staging_tables`
	* Load raw data from S3 buckets to Redshift staging tables
2. `insert_tables`
	* Transform staging table data to dimensional tables for data analysis

### [create_tables.py](create_tables.py)
Creating Staging, Fact and Dimension table schema

1. `drop_tables`
2. `create_tables`

### [sql_queries.py](sql_queries.py)
SQL query statement collecitons for `create_tables.py` and `etl.py`

1. `*_table_drop`
2. `*_table_create`
3. `staging_*_copy`
3. `*_table_insert`


## Database Schema
### Staging tables
```
staging_events
    event_id        BIGINT IDENTITY(0,1),
    artist          VARCHAR,
    auth            VARCHAR,
    firstName       VARCHAR,
    gender          VARCHAR,
    itemInSession   VARCHAR,
    lastName        VARCHAR,
    length          VARCHAR,
    level           VARCHAR,
    location        VARCHAR,
    method          VARCHAR,
    page            VARCHAR,
    registration    VARCHAR,
    sessionId       INTEGER SORTKEY DISTKEY,
    song            VARCHAR,
    status          INTEGER,
    ts              BIGINT,
    userAgent       VARCHAR,
    userId          INTEGER

staging_songs
    num_songs           INTEGER,
    artist_id           VARCHAR SORTKEY DISTKEY,
    artist_latitude     VARCHAR,
    artist_longitude    VARCHAR,
    artist_location     VARCHAR(500),
    artist_name         VARCHAR(500),
    song_id             VARCHAR,
    title               VARCHAR(500),
    duration            DECIMAL(9),
    year                INTEGER
```

### Fact table
```
songplays
    songplay_id     INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
    start_time      TIMESTAMP               NOT NULL,
    user_id         VARCHAR(50)             NOT NULL DISTKEY,
    level           VARCHAR(10)             NOT NULL,
    song_id         VARCHAR(40)             NOT NULL,
    artist_id       VARCHAR(50)             NOT NULL,
    session_id      VARCHAR(50)             NOT NULL,
    location        VARCHAR(100)            NULL,
    user_agent      VARCHAR(255)            NULL
```

### Dimension tables
```
users
    user_id         INTEGER         NOT NULL SORTKEY,
    first_name      VARCHAR(50)     NULL,
    last_name       VARCHAR(80)     NULL,
    gender          VARCHAR(10)     NULL,
    level           VARCHAR(10)     NULL

songs
    song_id     VARCHAR(50)     NOT NULL SORTKEY,
    title       VARCHAR(500)    NOT NULL,
    artist_id   VARCHAR(50)     NOT NULL,
    year        INTEGER         NOT NULL,
    duration    DECIMAL(9)      NOT NULL

artists
    artist_id   VARCHAR(50)             NOT NULL SORTKEY,
    name        VARCHAR(500)            NULL,
    location    VARCHAR(500)            NULL,
    latitude    DECIMAL(9)              NULL,
    longitude   DECIMAL(9)              NULL

time
    start_time  TIMESTAMP   NOT NULL SORTKEY,
    hour        SMALLINT    NULL,
    day         SMALLINT    NULL,
    week        SMALLINT    NULL,
    month       SMALLINT    NULL,
    year        SMALLINT    NULL,
    weekday     SMALLINT    NULL
```
