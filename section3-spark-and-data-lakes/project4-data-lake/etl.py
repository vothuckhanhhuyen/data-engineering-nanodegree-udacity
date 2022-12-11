import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import monotonically_increasing_id


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID'] = config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark session

    Arguments:
        None

    Returns:
        Spark session
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """ 
    Process song data and create songs and artists table

    Arguments:
        spark: Spark session
        input_data: Source S3 endpoint
        output_data: Target S3 endpoint

    Returns:
        None
    """
    # get filepath to song data file
    song_data = os.path.join(input_data + 'song_data/*/*/*/*.json')

    # read song data file
    df = spark.read.json(song_data)

    # create temp view of song data for songplays table to join
    df.createOrReplaceTempView("song_data_temp_view")

    # extract columns to create songs table
    songs_table = df.select('song_id', 'title', 'artist_id',
                            'year', 'duration').distinct()

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id')\
               .parquet(path=output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location',
                              'artist_latitude', 'artist_longitude').distinct()

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(path=output_data + 'artists')


def process_log_data(spark, input_data, output_data):
    """ 
    Process log data and create users, time and songplays table

    Arguments:
        spark: Spark session
        input_data: Source S3 endpoint
        output_data: Target S3 endpoint

    Returns:
        None
    """
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.where(df['page'] == 'NextSong')

    # extract columns for users table
    users_table = df.select('userId', 'firstName',
                            'lastName', 'gender', 'level').distinct()

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(path=output_data + 'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))

    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))

    # extract columns to create time table
    df = df.withColumn("hour", hour("start_time"))
    df = df.withColumn("day", dayofmonth("start_time"))
    df = df.withColumn("week", weekofyear("start_time"))
    df = df.withColumn("month", month("start_time"))
    df = df.withColumn("year", year("start_time"))
    df = df.withColumn("weekday", date_format("start_time", 'E'))
    time_table = df.select("start_time", "hour", "day", "week",
                           "month", "year", "weekday").distinct()

    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month')\
              .parquet(path=output_data + 'time')

    # read in song data to use for songplays table
    song_df = spark.sql("SELECT * FROM song_data_temp_view")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.join(song_df, (df.song == song_df.title)
                              & (df.artist == song_df.artist_name)
                              & (df.length == song_df.duration), "inner") \
        .distinct() \
        .select("start_time", "userId", "level", "song_id",
                "artist_id", "sessionId", "location", "userAgent",
                df['year'].alias('year'), df['month'].alias('month')) \
        .withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode("overwrite").partitionBy('year', 'month')\
                   .parquet(path=output_data + 'songplays')


def main():
    """
    Main function
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
