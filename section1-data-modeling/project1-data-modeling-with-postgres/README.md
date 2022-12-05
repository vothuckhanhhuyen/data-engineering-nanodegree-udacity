# Project: Data Modeling with Postgres

This project models user activity data for a music streaming app called Sparkify to optimize queries for understanding what songs users are listening to by creating a **Postgres relational database** and **ETL pipeline** to build up **Fact and Dimension tables** and insert data into new tables.


## Project Structure

```
Data Modeling with Postgres
|____data			# Dataset
| |____log_data
| | |____...
| |____song_data
| | |____...
|
|____etl.ipynb		        # developing ETL builder
|____test.ipynb		        # testing ETL 
|____etl.py	                # ETL builder
|____sql_queries.py		# ETL query helper 
|____create_tables.py		# database/table 
|____create_erd.py		# erd 
|____sparkifydb_erd.png	
|____dend-p1-lessons-cheat-sheet.pdf
|____README.md
```


## ETL Pipeline
### [etl.py](etl.py)
ETL pipeline builder

1. `process_data`
	* Iterating dataset to apply `process_song_file` and `process_log_file` functions
2. `process_song_file`
	* Process song dataset to insert record into _songs_ and _artists_ dimension table
3. `process_log_file`
	* Process log file to insert record into _time_ and _users_ dimensio table and _songplays_ fact table

### [create_tables.py](create_tables.py)
Creating Fact and Dimension table schema

1. `create_database`
2. `drop_tables`
3. `create_tables`

### [sql_queries.py](sql_queries.py)
Helper SQL query statements for `etl.py` and `create_tables.py`

1. `*_table_drop`
2. `*_table_create`
3. `*_table_insert`
4. `song_select`


## Database Schema
### Fact table
```
songplays
	- songplay_id 	PRIMARY KEY
	- start_time 	REFERENCES time (start_time)
	- user_id	REFERENCES users (user_id)
	- level
	- song_id 	REFERENCES songs (song_id)
	- artist_id 	REFERENCES artists (artist_id)
	- session_id
	- location
	- user_agent
```

### Dimension table
```
users
	- user_id 	PRIMARY KEY
	- first_name
	- last_name
	- gender
	- level

songs
	- song_id 	PRIMARY KEY
	- title
	- artist_id
	- year
	- duration

artists
	- artist_id 	PRIMARY KEY
	- name
	- location
	- latitude
	- longitude

time
	- start_time 	PRIMARY KEY
	- hour
	- day
	- week
	- month
	- year
	- weekday
```

## References
1. [fast-load-data-python-postgresql](https://hakibenita.com/fast-load-data-python-postgresql)
2. [Pandas to PostgreSQL using Psycopg2: Bulk Insert Performance Benchmark](https://naysan.ca/2020/05/09/pandas-to-postgresql-using-psycopg2-bulk-insert-performance-benchmark/)
