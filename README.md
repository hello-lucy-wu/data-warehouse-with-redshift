# data-warehouse-with-redshift

This project is to build a data warehouse based on [song data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/song_data/?region=us-west-2&tab=overview) and [log data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/log_data/?region=us-west-2&tab=overview) residing in S3. The data structure is the same as the one in my previous project  [data-modeling-with-postgres](https://github.com/hello-lucy-wu/data-modeling-with-postgres#Data). I created an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables to find insights in what is the most popular song in the database

### Table of Contents
* [Tables](#Tables)
* [Steps to run scripts](#Steps)

### Tables
* There are four dimension tables and one fact tables.
    - Fact Table \
        songplays - records in log data associated with song plays i.e. records with page NextSong songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

    - Dimension Tables \
        users - users in the app
        user_id, first_name, last_name, gender, level

        songs - songs in music database
        song_id, title, artist_id, year, duration

        artists - artists in music database
        artist_id, name, location, latitude, longitude

        time - timestamps of records in songplays broken down into specific units
        start_time, hour, day, week, month, year, weekday
* CREATE statements in `sql_queries.py` specify all columns for each of the five tables with data types and conditions.


### Steps 
* replace HOST and ARN in dwh.cfg with your own redshift cluster host and iam role
* run `create_tables.py` to create fact and dimension tables for the star schema in Redshift
* run `etl.py` to load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift
* run 'get_insights.py' to get most popular song
