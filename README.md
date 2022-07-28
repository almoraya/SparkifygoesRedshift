# Sparkify goes big: Data Warehousing with Redshift

Sparkify, a startup with a new streaming app, has recently seen an increase in its customer base and song database system. After discussions with an IT consulting firm, the company has decided to move its operations to the cloud. Sparkify hopes this will not only reduce costs, but also allow it to scale flexibly to meet its business needs.


### Table of Contents

- [Project Summary](#project)
- [Redshift: The Column-Based Database](#redshift)
- [How to run the Python Scripts](#python)
- [Sparkify Data Files](#files)
- [Design of the Database Schema and ETL Pipeline](#pipeline)
- [Querying the Data](#query)


<a name="project"/>

## Project Summary


After a steady growth of its user base, Sparkify decided to move its digital business operations to the cloud. They chose AWS as their cloud provider and S3 and Redshift as their main cloud services. S3 is AWS object storage solution for the cloud. Data in S3 is stored in containers called buckets, which are optimized for storing large amounts of unstructured data, making it ideal for storing all their song and log files. The data is then stored in Redshift, a fully managed cloud data warehouse service from AWS that is ideal for quickly and efficiently analyzing large amounts of data, merging data from many different sources, and analyzing your own data stored in S3.


<a name="redshift"/>

## Redshift: The Column-Based Database


A column-oriented DBMS or column-based DBMS stores its data by column rather than by row. Being designed and optimized to scale quickly with distributed clusters, or to retrieve columns quickly, column-oriented databases are usually used for data warehousing and Big Data processing. A Redshift database consists of at least one cluster node, which is composed of a leader node and one or more compute nodes.

Below is an excerpt from one of our database tables in Redshift, the users table. In row-oriented database system, data is stored each row at a time.

`users table:`

|user_id|first_name|last_name|gender|level|
|---|---|---|---|---|
|80|Tegan|Levine|F|paid|
|92|Ryann|Smith|F|free|
|74|Braden|Parker|M|free|
|55|Martin|Johnson|M|free|

However, in a column-based database system, data is stored in blocks that contain only columnar data. The primary purpose of a column-based database is to efficiently write data to and read data from disk storage in order to reduce the time it takes to respond to a query.

Block **user_Id** --> {80, 92, 74, 55}

Block **first_name** --> {Tegan, Ryann, Braden, Martin}

Block **last_name** --> {Levine, Smith, Parker, Johnson}

Block **gender** --> {F, F, M, M}

Block **level** --> {paid, free, free, free} 


To enable fast retrieval of query data at any time, these types of database systems are equipped with the ability to specify partition and clustering keys. In the case of Redshift, a number of settings are available to the data engineer to improve query performance when designing tables. These settings are:

- distribution, by evenly assigning data and processing tasks to each node
- sorting, by organizing the data on disk to minimize the number of disk reads
- compression, by reducing memory and disk I/O requirements

Since our data has high cardinality among the columns and data groupings, if any, are not evenly distributed among the groups, I decided to use Redshift's default distribution strategies


<a name="python"/>

## How to run the Python scripts

In order to be able to run the provided python scripts, the following criteria needs to be fulfilled:
- Python must be installed in version 3.6.3 or higher.
- It is highly recommended to install the necessary Python libraries in a virtual environment.
- Access to an AWS account with permission to access S3 buckets and deploy a Redshift cluster.
- Save all required string connection data in the *dwh.cfg* file

There are three main Python scripts used to create and populate the Redshift database required by Sparkify. These are:

1. the *sql_queries.py* script: this script gathers some of the main data manipulation language and data definition language commands needed to not only create our database, but also to populate it with data from the log and song files. Here we created two staging tables to be used as our landing zone, where our files could be used for processing during the extract, transform and load (ETL) process.
2. the *create_tables.py* script: in this script we make full use of the Python library psycopg2. This library allows us to connect to our Redshift database, run the queries stored in our sql_queries.py script, and commit all our changes to the database. This script is very important when we are trying to find bugs and test our improvements. This script must be run every time changes are made to the *sql_queries.py* script.
3. the *etl.py* script: once the tables are created, we proceed to extract, transform and load the data into our database. This script acts as an orchestration engine between the raw data and our Redshift database. It relies heavily on the queries we develop for the sql_queries.py script. Once this script is executed, the Redshift database is populated with clean data.


<a name="files"/>

## Sparkify Data Files

The files used in this repository are all stored in the **data** folder. This folder contains the song dataset and the log dataset, both in json format.

**The song dataset**

The song dataset is a subset of the [MillionDataSet](http://millionsongdataset.com/). These files contain real metadata about songs and artists. As mentioned earlier, it is a subset that contains only the first three letters of the track ID of each song; hence the partitioning.

**The log Dataset**

Unlike the song files, which are real data, the log files are simulated files. They were created using an external tool ([event simulator](https://github.com/Interana/eventsim)). These files are divided by month; however, generated data for the month of November was used.


<a name="pipeline"/>

## Design of the database schema and ETL pipeline

**Design of the database schema**

This database was developed to contain two staging tables, four dimension tables and one fact table. The data copied from Sparkify S3 buckets where uploaded into our two staging tables. These tables where used to derived the five following tables. The dimension tables were created by grouping related attributes about the numerical values in the fact table, under one dimension; this avoided duplicating data when it was not necessary. As a result, we have the following dimension tables:

- Songs
- Artists
- Users
- Time

The Songplays fact table was created to store partially denormalized data for analytical purposes. While the granularity of dimension tables is theoretically much higher, the fact table takes data with a finer grain.

**ETL Pipeline**

We wrote our ETL pipeline using Python. In doing so, we made extensive use of the **pyscopg2** library. This library allowed us to connect to our Redshit database, create a cursor to execute commands, and run all of our Python scripts from one place. To copy all the json files, perform the necessary transformations, and load the final data into our database, we created three Python scripts. Detailed explanations of the scripts and how they work can be found above in the section: **Execution of the Python Scripts**.


<a name="query"/>

## Querying the Data

This query shows us the total number of times a user has listened to a song, not the number of unique songs.
```
select concat(first_name, last_name) as full_name, count(sg.song_id) as totalsongslistened
from songplays sp
join users us on (sp.user_id = us.user_id)
join songs sg on (sp.song_id = sg.song_id)
group by first_name, last_name
order by totalsongslistened desc
limit 10;
```
<p align="center">
<img  width="50%" height= "50%" src=/images/result_query1.png alt="Result to query 1">
</p>

This query provides us with information about the distribution of our users in terms of level (subscription type).
```
select level as subscription_type, count(user_id) total_subscriptions_by_type
from users
group by level
order by total_subscriptions_by_type
limit 10;
```
<p align="center">
<img  width="50%" height= "50%" src=/images/result_query2.png alt="Result to query 2">
</p>

This last query provides us with information about who are the most listened artists.
```
select name as artist_name, count(sg.song_id) as total_songs_listened
from songplays sp
join songs sg on (sp.song_id = sg.song_id)
join artists ar on (sp.artist_id = ar.artist_id)
group by name
order by total_songs_listened desc
limit 10;
```
<p align="center">
<img  width="50%" height= "50%" src=/images/result_query3.png alt="Result to query 3">
</p>
