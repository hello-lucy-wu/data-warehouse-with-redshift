import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE "staging_events" (
        artist              text,
        auth                text,
        firstName           text,
        gender              VARCHAR(1),
        itemInSession       int,
        lastName            text,
        length              numeric,
        level               text,
        location            text,
        method              VARCHAR(3),
        page                text,
        registration        numeric,
        sessionId           int,
        song                text,
        status              int,
        ts                  BIGINT,
        userAgent           text,
        userId              int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE "staging_songs" (
        num_songs           int,
        artist_id           text,
        artist_latitude     numeric,
        artist_longitude    numeric,
        artist_location     text,
        artist_name         text,
        song_id             text,
        title               text,
        duration            numeric,
        year                int
    )
    diststyle all;
""")

songplay_table_create = ("""
    CREATE TABLE "songplays" (
        songplay_id         INT IDENTITY(0,1) PRIMARY KEY,
        start_time          TIMESTAMP,
        user_id             INTEGER NOT NULL,
        level               text NOT NULL,
        song_id             text NOT NULL,
        artist_id           text NOT NULL,
        session_id          INTEGER NOT NULL,
        location            text,
        user_agent          text
    );
""")

user_table_create = ("""
    CREATE TABLE "users" (
        user_id             INTEGER PRIMARY KEY,
        first_name          text,
        last_name           text,
        gender              VARCHAR(1),
        level               text NOT NULL
    )
    diststyle all;
""")

song_table_create = ("""
    CREATE TABLE "songs" (
        song_id             text PRIMARY KEY,
        title               text,
        artist_id           text NOT NULL,
        year                INTEGER,
        duration            numeric
    )
    diststyle all;
""")

artist_table_create = ("""
    CREATE TABLE "artists" (
        artist_id           text PRIMARY KEY,
        name                text,
        location            text,
        latitude            numeric,
        longitude           numeric
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE "time" (
        start_time          TIMESTAMP PRIMARY KEY sortkey,
        hour                INTEGER NOT NULL,
        day                 INTEGER NOT NULL,
        week                INTEGER NOT NULL,
        month               INTEGER NOT NULL,
        year                INTEGER NOT NULL,
        weekday             INTEGER NOT NULL
    )
    diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from '{}' 
    iam_role {} json {} region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from '{}' 
    iam_role {} json 'auto' region 'us-west-2';
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
insert into songplays (
        start_time,          
        user_id,             
        level,               
        song_id,             
        artist_id,           
        session_id,          
        location,            
        user_agent
)
(
    select 
        TIMESTAMP 'epoch' + staging_events.ts/1000 * interval '1 second', 
        CAST(staging_events.userId AS int),
        level,
        songs.song_id,
        artists.artist_id,
        staging_events.sessionId,
        staging_events.location,
        staging_events.userAgent
    from staging_events
    join songs on staging_events.song = songs.title
    join artists on staging_events.artist = artists.name
    where staging_events.page = 'NextSong'
);
""")

user_table_insert = ("""
insert into users ( 
    select 
        cast(userId as int),
        firstName,
        lastName,
        gender,
        level
    from staging_events
    where LENGTH(staging_events.userId) > 0
)
""")

song_table_insert = ("""
insert into songs ( 
    select 
        song_id,
        title,
        artist_id,
        year,
        duration
    from staging_songs
    where LENGTH(staging_songs.song_id) > 0
)
""")

artist_table_insert = ("""
insert into artists ( 
    select 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    from staging_songs
    where LENGTH(staging_songs.artist_id) > 0
)
""")

time_table_insert = ("""
insert into time ( 
    select 
        songplays.start_time,  
        date_part(hour, songplays.start_time), 
        date_part(day, songplays.start_time), 
        date_part(week, songplays.start_time), 
        date_part(month, songplays.start_time), 
        date_part(year, songplays.start_time), 
        date_part(weekday, songplays.start_time)
    from songplays
)
""")

get_most_popular_song_name = ("""
select 
    songs.song_id, 
    songs.title, 
    count(*) as counts
from songplays
join songs on songs.song_id = songplays.song_id
group by songs.song_id, songs.title
order by counts desc
limit 1
""")




# QUERY LISTS

create_table_queries = [user_table_create, staging_events_table_create, staging_songs_table_create, songplay_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, songplay_table_insert, time_table_insert]

get_insights_queries = [get_most_popular_song_name]
