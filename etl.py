import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['KEYS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['KEYS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .appName("Data Lake with Spark SQL") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
        
    # get filepath to song data file
    song_data = "s3a://udacity-dend/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    print(songs_table)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(output_data + "song")

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(output_data + "artist")


def process_log_data(spark, input_data, output_data):

    # get filepath to log data file
    log_data = "s3a://udacity-dend/log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page = 'NextSong'")

    # extract columns for users table    
    user_table = df.selectExpr("userId as user_id", \
                          "firstName as first_name",\
                          "lastName as last_name",\
                          "gender", \
                          "level").distinct()
    
    # write users table to parquet files
    user_table.write.mode("overwrite").parquet(output_data + "user")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: to_timestamp(from_unixtime(x/1000), TimestampType()))
    df = df.withColumn("start_time", get_timestamp("ts")) 
    
      
    # extract columns to create time table
    time_table = df.select("start_time")
    time_table = time_table.withColumn("year", year("start_time"))\
                            .withColumn("hour", hour("start_time"))\
                            .withColumn("month", month("start_time"))\
                            .withColumn("day", dayofmonth("start_time"))\
                            .withColumn("week", weekofyear("start_time"))\
                            .withColumn("weekday", date_format(col("start_time"), "EEEE"))

    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(output_data + "time")

    # read in song data to use for songplays table
    song_data = input_data + "song_data"    
    dfSongData = spark.read.json(song_data)
    songs_table = dfSongData.select(["song_id", "title", "artist_id", "year", "duration"]).distinct()

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.selectExpr("start_time",\
                              "song",\
                              "ts",\
                              "userId as user_id", \
                              "level", \
                              "sessionId as session_id", \
                              "location", \
                              "userAgent as user_agent")
   
    # join song with songplay data
    songplays_table = songplays_table.join(songs_table, songplays_table.song == songs_table.title, "left")
    
    # generate song play id
    window = Window.orderBy(col('ts'))
    songplays_table = songplays_table.withColumn('songplay_id', row_number().over(window))
    
    # add year and month columns for partition
    songplays_table = songplays_table.withColumn("songplay_year", year("start_time"))\
                                      .withColumn("month", month("start_time"))
    songplays_table = songplays_table.drop("song", "title", "duration", "ts", "song", "year")
    
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("songplay_year", "month").mode("overwrite").parquet(output_data + "songplay")


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://datalake-igor/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
