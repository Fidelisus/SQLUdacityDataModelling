## Data modeling SQL Project

The database stores users activity for company called Sparkify. It stores what songs do users listen to together with:
-Song data
-Artist data
-User data
-Session location, user agent and time

Star model of the database is used. It is a simple design model where queries are simple to make. It also supports data aggregations quite well, which is needed because the company wants to analyse this data. As our data is not that complicated, the simplest model is the best in this case.

We have a fact table called songplays storing information about every time a song was played. It connect the rest of tables in the database. It stores:
- songplay_id
- start_time
- user_id
- level
- song_id
- artist_id
- session_id
- location
- user_agent

There are also 4 dimension tables:
- Songs table- storing information about songs
- Artists table- storing inforation about artists who made songs
- Users table- storing information about users in the database
- Times table- stores information when the song from songplays table was played

In the project we have following files:
- sql_queries.py- all necesarry PostgreSQL queires are there.
- create_tables.py- it deletes the database and creates a new one with empty tables. Queries for them are included in sql_queries.py.
- etl.py- contains ETL pipeline that extract all the data from the files provided by Sparkify and puts it into the database.

The ETL pipeline converts all the data gathered by the company in JSON files and puts it in the database. It is consisted of following steps (for every table):
1. Reading JSON data files provided by Sparkify
2. Converting JSON files to pandas DataFrames
3. Inserting data to database