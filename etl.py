import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import IntegerType
from pyspark.sql import Row, functions as F
from pyspark.sql.window import Window
import time
import datetime


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
     print("---[ song_data ]---")
    # get filepath to song data file
    song_data = input_data + "song_data/*/*/*"

    # read song data file
    df_SongData = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df_song_data.filter('song_id != ""').select(['song_id', 'title', 'artist_id', 'year', 'duration']).dropna(how = "any", subset = ["song_id"]).dropDuplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy('year', 'artist_id').parquet(output_data + "songs")

    # extract columns to create artists table
    artists_table = df_SongData.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude',
                              'artist_longitude').where(df_SongData.artist_id !='').dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists")


def process_log_data(spark, input_data, output_data):
    print("---[ log_data ]---")

    # get filepath to log data file
    log_data = input_data

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log =  df_log.filter(col("page") == 'NextSong')

    # extract columns for users table    
    user_table = df_log.sort(df_log.ts.desc()).select('userId','firstName', 'lastName', 'gender','level').where(df_log.userId!='').dropDuplicates(['userId'])
    # write users table to parquet files
    user_table.write.parquet(output_data + "users")
    
    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df_log = df_log.withColumn("timestamp", get_timestamp("ts"))
    
    #get_timestamp = udf(lambda x: datetime.datetime.fromtimestamp(int(x / 1000)).strftime('%Y-%m-%d %H:%M:%S'))
    #df_log = df_log.withColumn( "timestamp", to_timestamp(get_timestamp(df_log.ts)))

    # create datetime column from original timestamp column

    get_datetime = udf(lambda x: datetime.datetime.fromtimestamp(int(x / 1000)).strftime('%Y-%m-%d %H:%M:%S'))
    df_log = df_log.withColumn( "datetime", get_datetime(df_log.ts))
    
    # extract columns to create time table
    time_table = time_table.select(col("timestamp").alias("start_time"),
                                   hour(col("datetime")).alias("hour"),
                                   dayofmonth(col("datetime")).alias("day"), 
                                   weekofyear(col("datetime")).alias("week"), 
                                   month(col("datetime")).alias("month"),
                                   year(col("datetime")).alias("year"),
                                  date_format("datetime", 'F').alias("weekday"))
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year","month").parquet(output_data+"time")

    # read in song data to use for songplays table
    song_df = spark.read.json(input_data + "song_data/*/*/*/*.json")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = song_df.select('start_time','userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location','userAgent')
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id()).withColumn('month', month(df.start_time)).withColumn('year', year(df.start_time))

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year","month").parquet(output_data+"songplays")


def main():
    spark = create_spark_session()
    input_data = "s3a://input-udacity-dend/"
    output_data = "s3a://output-udacity-dend/"
   # input_data = "./data/"
   # output_data = "./data/output/" 
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)

         print("---[ completed! ]---")


if __name__ == "__main__":
    main()
