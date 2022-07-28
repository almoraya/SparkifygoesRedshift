import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events;"
staging_songs_table_drop = "drop table if exists staging_songs;"
songplay_table_drop = "drop table if exists songplays;"
user_table_drop = "drop table if exists users;"
song_table_drop = "drop table if exists songs;"
artist_table_drop = "drop table if exists artists;"
time_table_drop = "drop table if exists time;"


### https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html
### https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html
### Foreign key constraints are informational only; they are not enforced by Amazon Redshift.

### Since our data has high cardinality among the columns and data groupings, if any, 
### are not evenly distributed among the groups, I decided to use Redshift's default distribution strategies

# CREATE TABLES

staging_events_table_create= ("""
    create table if not exists staging_events
        (
            artist              varchar,	
            auth	            varchar,
            firstName	        varchar,
            gender              char(1),	
            itemInSession	    smallint,
            lastName	        varchar,
            length	            double precision,
            level               varchar,	
            location	        varchar,
            method	            char(3),
            page	            varchar,
            registration	    bigint,
            sessionId	        integer,
            song	            varchar,
            status	            smallint,
            ts	                bigint,
            userAgent	        varchar,
            userId              integer
        );
""")

staging_songs_table_create = ("""
    create table if not exists staging_songs
        (
            num_songs           integer,   	
            artist_id	        varchar(20),
            artist_latitude     double precision,	
            artist_longitude	double precision,
            artist_location	    varchar,
            artist_name	        varchar,
            song_id	            varchar(19),
            title	            varchar,
            duration	        double precision,
            year                smallint
        );

""")

songplay_table_create = ("""
    create table if not exists songplays
        (
            songplay_id         integer             identity(1, 1),
            start_time          timestamp,
            user_Id             integer,
            level               varchar,
            song_id             varchar(19),
            artist_id           varchar(20),
            session_id          integer,
            location            varchar,
            user_agent          varchar,
            primary key (songplay_id)
        );
""")

user_table_create = ("""
    create table if not exists users
        (
            user_id             integer,
            first_name          varchar,
            last_name           varchar,
            gender              char(1),
            level               varchar,
            primary key (user_id)
        );
""")

song_table_create = ("""
    create table if not exists songs
        (
            song_id             varchar,
            title               varchar,
            artist_id           varchar,
            year                smallint,
            duration            double precision,
            primary key (song_id)
        );
""")

artist_table_create = ("""
    create table if not exists artists
        (
            artist_id           varchar,
            name                varchar,
            location            varchar,
            latitude            double precision,
            longitude           double precision,
            primary key (artist_id)
        );
""")

time_table_create = ("""
    create table if not exists time
        (
            start_time          timestamp,
            hour                integer,
            day                 integer,
            week                integer,
            month               integer,
            year                integer,
            weekday             integer,
            primary key (start_time)
        );
""")


# STAGING TABLES

### If the Amazon S3 buckets that hold the data files don't reside in the same AWS Region as your cluster, 
### you must use the REGION parameter to specify the Region in which the data is located. (Amazon Redshift Documentation)

### A JSONPaths file is a text file that contains a single JSON object with the name "jsonpaths" paired with an array of JSONPath expressions.
### https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#r_COPY_command_examples-copy-from-json 

### select count(*) from staging_events = 8056
staging_events_copy = ("""
    copy staging_events from {} 
    iam_role '{}'
    json {}             
    region 'us-west-2';
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])


### https://docs.aws.amazon.com/redshift/latest/dg/copy-usage_notes-copy-from-json.html
### 'auto' – COPY automatically loads fields from the JSON file.
 
### select count(*) from staging_songs =  14896
staging_songs_copy = ("""
    copy staging_songs from {}
    iam_role '{}'
    json 'auto'
    region 'us-west-2'
""").format(config['S3']['SONG_DATA'], config['IAM_ROLE']['ARN'])


# FINAL TABLES

### https://docs.aws.amazon.com/redshift/latest/dg/r_Dateparts_for_datetime_functions.html
### timestamp with time zone 'epoch' + pgdate_part * interval '1 second' AS converted_timestamp

# select count(*) from songplays; = 333
songplay_table_insert = ("""
    insert into songplays (start_time, user_Id, level, song_id, 
                           artist_id, session_id, location, user_agent)
    select distinct
                    timestamp 'epoch' + (se.ts/1000) * interval '1 second',
                    se.userId,
                    se.level,
                    ss.song_id,
                    ss.artist_id,
                    se.sessionId,
                    se.location,
                    se.userAgent
    from staging_events se
    inner join staging_songs ss 
    on (
        se.artist = ss.artist_name
        and se.song = ss.title
        )
    where se.page = 'NextSong';
""")


### https://www.navicat.com/en/company/aboutus/blog/1647-applying-select-distinct-to-one-column-only
### here we include a sub-query that group

### select count(*) from users; = 67
user_table_insert = ("""
    insert into users (user_id, first_name, last_name, gender, level)
    select 
            userId,
            firstName,
            lastName,
            gender,
            level
    from staging_events
    where page = 'NextSong'
    and ts in (
        select max(ts)
        from staging_events
        group by userId);

""")

### select count(*) from songs; = 14896
song_table_insert = ("""
    insert into songs (song_id, title, artist_id, year, duration)
    select distinct
            song_id,
            title,
            artist_id,
            year,
            duration
    from staging_songs; 
""")

### select count(*) from artists; = 10025
artist_table_insert = ("""
    insert into artists (artist_id, name, location, latitude, longitude)
    select distinct
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    from staging_songs;
""")

### https://docs.aws.amazon.com/redshift/latest/dg/r_EXTRACT_function.html
### https://docs.aws.amazon.com/redshift/latest/dg/r_Dateparts_for_datetime_functions.html
### day of week returns an integer from 0–6, starting with Sunday.

### select count(*) from time; = 6813
time_table_insert = ("""
    insert into time (start_time, hour, day, week, month, year, weekday)
    select distinct
        timestamp 'epoch' + (ts/1000) * interval '1 second',
        extract(hour from timestamp 'epoch' + (ts/1000) * interval '1 second'),
        extract(day from timestamp 'epoch' + (ts/1000) * interval '1 second'),
        extract(week from timestamp 'epoch' + (ts/1000) * interval '1 second'),
        extract(month from timestamp 'epoch' + (ts/1000) * interval '1 second'),
        extract(year from timestamp 'epoch' + (ts/1000) * interval '1 second'),
        extract(weekday from timestamp 'epoch' + (ts/1000) * interval '1 second')
    from staging_events
    where page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]