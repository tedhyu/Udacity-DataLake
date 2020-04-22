import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
#from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import types as T


config = configparser.ConfigParser()
config.read_file(open('dl.cfg'))

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']



def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = input_data + 'song_data'
    # read song data file
    df = spark.read.json(song_data)
    #df.printSchema()
    
    df.createOrReplaceTempView("song_stage")

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM song_stage
    """)
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy('year','artist_id').parquet(output_data+'songs')

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM song_stage
    """)
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet(output_data+'artists')

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = input_data + 'log_data'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.filter("page == 'NextSong'")
    get_timestamp = udf(lambda x: datetime.fromtimestamp( (x/1000.0) ), T.TimestampType()) 
    df = df.withColumn("start_time", get_timestamp(df.ts))
    df.createOrReplaceTempView("log_stage")

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT userID, firstName, lastName, gender, level
        FROM  log_stage
    """)
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet(output_data+'users')

    # create timestamp column from original timestamp column
    
    # extract columns to create time table

    time_table = spark.sql("""
        SELECT DISTINCT start_time, 
                        hour(start_time) AS hour,
                        day(start_time)  AS day,
                        weekofyear(start_time) AS week,
                        month(start_time) AS month,
                        year(start_time) AS year,
                        dayofweek(start_time) AS weekday
        FROM  log_stage
    """)                  
        
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'time')
    time_table.createOrReplaceTempView("time_table")

    # read in song data to use for songplays table
    song_df = spark.read.parquet(output_data + 'songs')
    song_df.createOrReplaceTempView("song_table")
    song_df

#monotonically_increasing_id()
    # extract columns from joined song and log datasets to create songplays table 

    songplays_table = spark.sql("""
        SELECT DISTINCT l.start_time,
                        month(l.start_time) AS month,
                        year(l.start_time) AS year,
                        l.userID, 
                        s.song_id, 
                        s.artist_id, 
                        l.sessionId, 
                        l.location, 
                        l.userAgent 
        FROM  log_stage l, song_table s
        WHERE  l.song = s.title 
    """)          

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.mode('overwrite').partitionBy('year','month').parquet(output_data+'song_plays')
    

def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://udacity-tedyu/project4/"  

    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)
    # get filepath to log data file

if __name__ == "__main__":
    main()
