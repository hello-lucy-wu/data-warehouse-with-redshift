# data-warehouse-with-redshift

This project is to build a data warehouse based on [song data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/song_data/?region=us-west-2&tab=overview) and [log data](https://s3.console.aws.amazon.com/s3/buckets/udacity-dend/log_data/?region=us-west-2&tab=overview) residing in S3. The data structure is the same as the one in my previous project  [data-modeling-with-postgres](https://github.com/hello-lucy-wu/data-modeling-with-postgres#Data). I created an ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables to find insights in what songs people are listening to and how many songs do users listen to on average between visiting home page.
