# Base analysis

```sh
We have two big data file - song_data with songs and artist data, 
and log_data with streaming data of users events.

Our goal is create one fact table songsplays with time data and 
four dimensions table with other data with Spark (users, songs, artists, time).
```

# 1. Configure dl.cfg

```sh
Configure with parameters of IAM and AWS access key
```

# 2. Write process_song_data and process_log_data to skript etl.py

```sh
File will process with data frame and write to s3 in parquet files

Song table was partitied by year and artist_id

Time table was partitied by year and month
```

# 3. Run etl.py 

```sh
Over console - !python etl.py or run over terminal python etl.py
```


