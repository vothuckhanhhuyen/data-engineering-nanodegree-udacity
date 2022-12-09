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

staging_songs
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
```

### Fact table
```
songplays
    songplay_id     INT IDENTITY(0,1) NOT NULL SORTKEY,
    start_time      TIMESTAMP NOT NULL,
    user_id         INT NOT NULL DISTKEY,
    level           VARCHAR(10) NOT NULL,
    song_id         VARCHAR(40) NOT NULL,
    artist_id       VARCHAR(50) NOT NULL,
    session_id      INT NOT NULL,
    location        VARCHAR(500),
    user_agent      VARCHAR(500)
```

### Dimension tables
```
users
    user_id         INT NOT NULL SORTKEY,
    first_name      VARCHAR(80),
    last_name       VARCHAR(80),
    gender          CHAR(1),
    level           VARCHAR(10)

songs
    song_id     VARCHAR(40) NOT NULL SORTKEY,
    title       VARCHAR NOT NULL,
    artist_id   VARCHAR(50) NOT NULL,
    year        INT NOT NULL,
    duration    FLOAT NOT NULL

artists
    artist_id   VARCHAR(50) NOT NULL SORTKEY,
    name        VARCHAR,
    location    VARCHAR(500),
    latitude    FLOAT,
    longitude   FLOAT

time
    start_time  TIMESTAMP NOT NULL SORTKEY,
    hour        SMALLINT,
    day         SMALLINT,
    week        SMALLINT,
    month       SMALLINT,
    year        SMALLINT,
    weekday     VARCHAR
```
