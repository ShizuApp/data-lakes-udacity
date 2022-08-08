import configparser
import os
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import TimestampType
from datetime import datetime


def create_spark_session():
    """
    Create and return a Spark session with hadoop-aws configuration
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Read song data and create the songs and artists tables

    ### Parameters
    - spark: SparkSession
    - input_data: input data path
    - output_data: output path
    """
    # get filepath to song data file
    song_data = input_data + 'song-data/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    songs_table = songs_table.drop_duplicates(subset=['song_id'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year', 'artist_id').parquet(output_data + 'songs_table')

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    artists_table = artists_table.drop_duplicates(subset=['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data + 'artists_table')


def process_log_data(spark, input_data, output_data):
    """
    Read log data and create the user, time, and songsplay tables

    ### Parameters
    - spark: SparkSession
    - input_data: input data path
    - output_data: output path
    """
    # get filepath to log data file
    log_data = input_data + 'log-data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where('page="NextSong"')

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"]).drop_duplicates(subset=['userId'])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data + 'users_table')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(col('ts')))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000).strftime('%y-%m-%d'))
    df = df.withColumn('datetime', get_datetime(col('ts')))
    
    # extract columns to create time table
    time_table = df.select(col('timestamp').alias('start_time')) \
                .withColumn('hour', F.hour('start_time')) \
                .withColumn('day', F.dayofmonth('start_time')) \
                .withColumn('week', F.weekofyear('start_time')) \
                .withColumn('month', F.month('start_time')) \
                .withColumn('year', F.year('start_time')) \
                .withColumn('weekday', F.dayofweek('start_time')) \
                .distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'time_table')

    # read in song data to use for songplays table
    song_data = input_data + 'song-data/*/*/*.json'
    
    # read song data file
    song_df = spark.read.json(song_data)

    # extract columns from joined song and log datasets to create songplays table 
    stage_df = df.join(song_df, (df.song == song_df.title) & (df.artist == song_df.artist_name), 'inner')

    songplays_table = \
    stage_df.distinct().select(
            col('timestamp').alias('start_time'), 
            col('userId').alias('user_id'), 
            col('level'),
            col('song_id'), 
            col('artist_id'),
            col('sessionId').alias('session_id'),
            col('location'), 
            col('userAgent').alias('user_agent')) \
            .withColumn('year', F.year('start_time')) \
            .withColumn('month', F.month('start_time')) \
            .withColumn('songplay_id', F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year', 'month').parquet(output_data + 'songplays_table')


def main():
    spark = create_spark_session()

    # read configuration
    config = configparser.ConfigParser()
    config.read('dl.cfg')

    # set aws credentials
    os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
    os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']

    input_data = config['S3']['INPUT_PATH'] # data input path
    output_data = config['S3']['OUTPUT_PATH'] # data output path

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
