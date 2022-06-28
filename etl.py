import configparser
from datetime import datetime
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T
from zipfile import ZipFile


config = configparser.ConfigParser()

# Read the configuration file.
config.read_file(open('dl.cfg'))

os.environ["AWS_ACCESS_KEY_ID"]= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ["AWS_SECRET_ACCESS_KEY"]= config['AWS']['AWS_SECRET_ACCESS_KEY']


AWS_ACCESS_KEY_ID=config['AWS']['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY= config['AWS']['AWS_SECRET_ACCESS_KEY']
# For run on Spark ERM to get data and store by using AWS S3
INPUT_DATA_SONG_AWS = config['AWS']['INPUT_DATA_SONG_AWS']
INPUT_DATA_LOG_AWS = config['AWS']['INPUT_DATA_LOG_AWS']
OUTPUT_DATA = config['AWS']['OUTPUT_DATA']

 # For run on local computer to get data and store on local folder
SONG_DATA_LOCAL=config['LOCAL']['INPUT_DATA_SONG_LOCAL']
LOG_DATA_LOCAL=config['LOCAL']['INPUT_DATA_LOG_LOCAL']
OUTPUT_DATA_LOCAL=config['LOCAL']['OUTPUT_DATA_LOCAL']

def unzip_data():
    """
    Function : Unzip the 2 song_data and log_data zip file to 2 folder data.
    Return : the 2 folder data
    """
    songdataZip = "data/song-data.zip"
    logdataZip = "data/log-data.zip"
    with ZipFile(songdataZip, 'r') as zip_song:
        # printing all the contents of the zip file
        zip_song.printdir()
        # extracting all the files
        print('Extracting all the files song data now...')
        zip_song.extractall("data/")
        print('Done!')
    with ZipFile(logdataZip, 'r') as zip_log:
        # printing all the contents of the zip file
        zip_log.printdir()
        # extracting all the files
        print('Extracting all the files of log data now...')
        zip_log.extractall("data/log_data")
        print('Done!')
    

def create_spark_session():
    """
    Function to create a Spark session.
    Return : the spark parameter.
    """
    print("Creating Spark Session...")
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    print("Success to create the SparkSession")
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Function : processing song data for store to parquet tables.
    Input : 
        - spark : the Spark session that will be use to execute commands and query SQL session.
        - input_data : The data input for processing song data. Link of wed S3 or folder data
        - output_data : The link of S3 or local folder to store the parquet tables.
    Return : The song table, artist table
    """
    # get filepath to song data file
    song_data = input_data
    try:
        # read song data file
        df_song = spark.read.json(song_data)
        # For check the schema of data
        df_song.printSchema()
    except:
        print("Error read data on link: ",input_data)
        print("The function auto unzip file will be auto run now ")
        unzip_data()
        df_song = spark.read.json(song_data)
        df_song.printSchema()

    """
    Create song_table process
    """
    # extract columns to create songs table
    song_cols = ['song_id', 'title', 'artist_id', 'year', 'duration']
    try:
        # To remove duplicate by selecting distinct song_id's
        song_table_1 = df_song.select(F.col('song_id'), 'title') \
            .groupBy('song_id') \
            .agg({'title': 'first'}) \
            .withColumnRenamed('first(title)', 'title1')
        # Create table for containing all the columns
        song_table_2 = df_song.select(song_cols)
        
        #
        print("created table 1 and table 2 is DONE ")

    except:
        print("Error to create Table 1 or table 2")

    try:
        # join table 1 and table to create songs table
        song_table_df = song_table_1.join(song_table_2, 'song_id') \
            .where(F.col("title1") == F.col("title")) \
            .select(song_cols)
        print("Join song tables is DONE")
        # Print the song table to check result
        song_table_df.toPandas().head()
        # write songs table to parquet files partitioned by year and artist
        song_table_df.write.parquet(output_data + 'songs_table',
                                    partitionBy=['year', 'artist_id'],
                                    mode='Overwrite')
        print("written songs to ", output_data,
              " 'songs_table' to parquet is DONE")
    except:
        print("Error to write song table to parquet files partitioned ")

    """
    Create artists table process
    """
    # define the columns
    artists_cols = ["artist_id", "artist_name",
                    "artist_location", "artist_latitude", "artist_longitude"]
    artists_table_1 = df_song.select(F.col('artist_id'), 'artist_name') \
        .groupBy('artist_id') \
        .agg({'artist_name': 'first'}) \
        .withColumnRenamed('first(artist_name)', 'artist_name1')

    artists_table_2 = df_song.select(artists_cols)
    # extract columns to create artists table
    try:
        artists_table_df = artists_table_1.join(artists_table_2, 'artist_id') \
            .where(F.col("artist_name1") == F.col("artist_name")) \
            .select(artists_cols)
        artists_table_df.toPandas().head()
        print("join artists_tables is Done")
        # write artists table to parquet files
        artists_table_df.write.parquet(
            output_data + 'artists_table', mode='Overwrite')
        print("Written artists table ", output_data,
              " 'artists_table' to parquet is DONE")
    except:
        print("Error to join artists tables")


def process_log_data(spark, input_data, output_data):
    """
    The Function processes log data
    Input :
        - spark : the Spark session that will be use to execute commands and query SQL session.
        - input_data : The data input for processing logging data. Link of wed S3 or folder data
        - output_data : The link of S3 or local folder to store the parquet tables.
    Return : The user table, time table and song_plays table.
    """
    # get filepath to log data file
    log_data = input_data
    print("Load song data from : ",input_data)
    # read log data file
    df_log = spark.read.json(input_data)
    df_log.printSchema()

    # filter by actions for song plays
    df_log.filter(F.col("page") == "NextSong").toPandas().head()
    df_log = df_log.filter(F.col("page") == "NextSong")
    df_log.toPandas().shape

    """
    Create Users table process
    """
    # extract columns for users table
    users_cols = ["userId", "firstName", "lastName", "gender", "level"]
    # print users_cols Head
    df_log.select(users_cols).toPandas().head()
    df_log.select(users_cols).toPandas().shape
    df_log.select(users_cols).dropDuplicates().toPandas().shape
    df_log.select(users_cols).dropDuplicates().toPandas().head()
    users_table_df = df_log.select(users_cols).dropDuplicates()
    # write users table to parquet files
    try:
        users_table_df.write.parquet(
            output_data + 'users_table', mode='Overwrite')
        print("Write users table ", output_data,
              " 'users_table' to parquet is DONE")
    except:
        print("Error write users table to parquet files")

    """
    Create Time table process
    """
    #create timestamp column from original timestamp column
    get_timestamp = udf()
    df_log.select('ts').toPandas().head()
    df_log.withColumn('ts', F.date_format(df_log.ts.cast(dataType=T.TimestampType()), "yyyy-MM-dd")).toPandas().head()
    
    # define functions for extracting time components from ts field
    get_hour = F.udf(lambda x: x.hour, T.IntegerType())
    get_day = F.udf(lambda x: x.day, T.IntegerType())
    get_week = F.udf(lambda x: x.isocalendar()[1], T.IntegerType())
    get_month = F.udf(lambda x: x.month, T.IntegerType())
    get_year = F.udf(lambda x: x.year, T.IntegerType())
    get_weekday = F.udf(lambda x: x.weekday(), T.IntegerType())
    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda x: datetime.fromtimestamp(
        (x/1000.0)), T.TimestampType())

    # create datetime column from original timestamp column
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    df_log = df_log.withColumn("hour", get_hour(df_log.timestamp))
    df_log = df_log.withColumn("day", get_day(df_log.timestamp))
    df_log = df_log.withColumn("week", get_week(df_log.timestamp))
    df_log = df_log.withColumn("month", get_month(df_log.timestamp))
    df_log = df_log.withColumn("year", get_year(df_log.timestamp))
    df_log = df_log.withColumn("weekday", get_weekday(df_log.timestamp))
    df_log.limit(5).toPandas()

    # extract columns to create time table
    time_cols = ["timestamp", "hour", "day",
                 "week", "month", "year", "weekday"]
    time_table_df = df_log.select(time_cols)
    # write time table to parquet files partitioned by year and month
    try:
        time_table_df.write.parquet(output_data + 'time_table',
                                    partitionBy=['year', 'month'],
                                    mode='Overwrite')
        print("Write time table to ", output_data,
              " parquet files partitioned is DONE")
    except:
        print("Error to write time table to parquet files ")

    # read the partitioned data song data to use for song plays table
    df_artists_read = spark.read.option(
        "mergeSchema", "true").parquet(output_data + "artists_table")
    df_songs_read = spark.read.option(
        "mergeSchema", "true").parquet(output_data + "songs_table")
    df_artists_read.toPandas().head()
    df_songs_read.toPandas().head()
    # merge song and artists
    df_songs_read.join(df_artists_read, 'artist_id').toPandas().head()
    # extract columns from joined song and log datasets to create songplays table
    df_joined_songs_artists = df_songs_read.join(df_artists_read, 'artist_id').select(
        "artist_id", "song_id", "title", "artist_name")
    songPlay_cols = ["timestamp", "userId", "song_id", "artist_id",
                     "sessionId", "location", "userAgent", "level", "month", "year"]
    # join df_logs with df_joined_songs_artists
    df_log.join(df_joined_songs_artists, df_log.artist ==
                df_joined_songs_artists.artist_name).select(songPlay_cols).toPandas().head()

    # extract columns from joined song and log datasets to create songplays table
    songPlay_table_df = df_log.join(
        df_joined_songs_artists, df_log.artist == df_joined_songs_artists.artist_name).select(songPlay_cols)
    songPlay_table_df = songPlay_table_df.withColumn(
        "songplay_id", F.monotonically_increasing_id())
    songPlay_table_df.toPandas().head()

    # write song plays table to parquet files partitioned by year and month
    try:
        songPlay_table_df.write.parquet(
            output_data + 'time_table', partitionBy=['year', 'month'], mode='Overwrite')
        print("Write songPlay Table to ", output_data,
              " parquet files partitioned is DONE")
    except:
        print("Error to create the songPlay Table")

def main():
    
    """
    Function : main function to execute application to process song data and log data
    
    """
    spark = create_spark_session()
    print('Starting Processing ..........')
    print('Start process SONG data')
    process_song_data(spark, SONG_DATA_LOCAL, OUTPUT_DATA_LOCAL)
    print('Done for processing SONG Data')
    print('Start process LOG data')
    process_log_data(spark, LOG_DATA_LOCAL, OUTPUT_DATA_LOCAL)
    print('Done for processing LOG Data')
    print('Finished Processing ..........')

if __name__ == "__main__":
    main()
