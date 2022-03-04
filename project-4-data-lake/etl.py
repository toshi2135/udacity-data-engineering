import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import TimestampType, DateType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
    Create Spark session.
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Process song_data using Spark.
    
    :param spark: Spark session
    :param input_data: input data path
    :param output_data: output data path
    """
    # get filepath to song data file
    song_data = input_data + "song_data/A/A/A/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").format("parquet").save(output_data + "songs_table.parquet")

    # extract columns to create artists table
    artists_table = df.selectExpr(["artist_id", \
                                   "artist_name AS name", \
                                   "artist_location AS location", \
                                   "artist_latitude AS latitude", \
                                   "artist_longitude AS longtitude"])
    
    # write artists table to parquet files
    artists_table.write.format("parquet").save(output_data + "artists_table.parquet")


def process_log_data(spark, input_data, output_data):
    """
    Process log_data using Spark
    
    :param spark: Spark session
    :param input_data: input data path
    :param output_data: output data path
    """
    # get filepath to log data file
    log_data = input_data + "log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter(df.page == 'NextSong')

    # extract columns for users table    
    users_table = df.selectExpr(["userId AS user_id", \
                                 "firstName AS first_name", \
                                 "lastName AS last_name", \
                                 "gender", \
                                 "level"])
    
    # write users table to parquet files
    users_table.write.format("parquet").save(output_data + "users_table.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df = df.withColumn('timestamp', get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x/1000.0), DateType())
    df = df.withColumn('datetime', get_datetime(df.ts))
    
    # extract columns to create time table
    time_table = df.selectExpr(["timestamp AS start_time", \
                            "hour(timestamp) AS hour", \
                            "dayofmonth(datetime) AS day", \
                            "weekofyear(datetime) AS week", \
                            "month(datetime) AS month", "year(datetime) AS year"])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").format("parquet").save(output_data + "time_table.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + "songs_table.parquet")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df \
    .join(song_df, [df.song == song_df.title, df.length == song_df.duration], "left") \
    .selectExpr(["timestamp AS start_time", \
                "userId AS user_id", \
                "level", \
                "song_id", \
                "artist_id", \
                "sessionId AS session_id", \
                "location", \
                "userAgent AS user_agent", \
                "month(datetime) AS month", \
                 "year(datetime) AS year"])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").format("parquet").save(output_data + "songplays_table.parquet")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = ""
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
