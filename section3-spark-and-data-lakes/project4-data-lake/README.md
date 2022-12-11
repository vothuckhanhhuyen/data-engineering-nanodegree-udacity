# Project: Spark and Data Lake
This project builds an **ETL pipeline** for a **data lake**. The data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in the app. We loaded data from S3, process the data into analytics tables using **Spark**, and load them back into S3.


## Project Structure

```
Spark and Data Lake
|____etl.py              # ETL builder
|____dl.cfg              # AWS configuration file
|____test.ipynb          # testing
```


## ELT Pipeline
### [etl.py](etl.py)
ELT pipeline builder

1. `process_song_data`
	* Load raw data from S3 buckets to Spark stonealone server and process song dataset to insert record into _songs_ and _artists_ dimension table

2. `process_log_data`
	* Load raw data from S3 buckets to Spark stonealone server and Process event(log) dataset to insert record into _time_ and _users_ dimensio table and _songplays_ fact table


## Database Schema

### Fact table

#### songplays table

```
root
 |-- start_time: timestamp (nullable = true)
 |-- userId: string (nullable = true)
 |-- level: string (nullable = true)
 |-- song_id: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- sessionId: long (nullable = true)
 |-- location: string (nullable = true)
 |-- userAgent: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- songplay_id: long (nullable = false)
```
partitionBy("year", "month")


### Dimension tables

#### user table

```
root
 |-- userId: string (nullable = true)
 |-- firstName: string (nullable = true)
 |-- lastName: string (nullable = true)
 |-- gender: string (nullable = true)
 |-- level: string (nullable = true)
```

#### songs table

```
root
 |-- song_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- artist_id: string (nullable = true)
 |-- year: long (nullable = true)
 |-- duration: double (nullable = true)
```

#### artists table

```
root
 |-- artist_id: string (nullable = true)
 |-- artist_name: string (nullable = true)
 |-- artist_location: string (nullable = true)
 |-- artist_latitude: double (nullable = true)
 |-- artist_longitude: double (nullable = true)
```

#### time table

```
root
 |-- start_time: timestamp (nullable = true)
 |-- weekday: string (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
 |-- week: integer (nullable = true)
 |-- day: integer (nullable = true)
 |-- hour: integer (nullable = true)
```
