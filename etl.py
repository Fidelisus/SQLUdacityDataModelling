import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Processes the song file from the filepath provided as the input.
    It extracts information from the file in order to put it into the database.
    It inserts records to songs and artists tables.

    INPUTS:
    * cur the database cursor
    * filepath the file path to the song file
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df[['song_id', 'title', 'artist_id', 'year',
                    'duration']].values[0]
    song_data = song_data.tolist()
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = df[['artist_id', 'artist_latitude', 'artist_location',
                      'artist_longitude', 'artist_name']].values[0]
    artist_data = artist_data.tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Processes the log file from the filepath provided as the input.
    It extracts information from the file in order to put it into the database.
    It reads time from ts atribute of JSON file,
    then extracts hour, day, week, month, year and weekday
    and insets it into times table.
    It also reads information about users contained in the file
    and inserts in in the users table.
    It also inserts information about the song played into the songplays table.

    INPUTS:
    * cur the database cursor
    * filepath the file path to the log file
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'])

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year,
                 t.dt.weekday]
    column_labels = ('start_time', 'hour', 'day', 'week', 'month', 'year',
                     'weekday')
    time_df = pd.DataFrame({column_labels[0]: time_data[0],
                            column_labels[1]: time_data[1],
                            column_labels[2]: time_data[2],
                            column_labels[3]: time_data[3],
                            column_labels[4]: time_data[4],
                            column_labels[5]: time_data[5],
                            column_labels[6]: time_data[6]}
                           )

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = df[['ts', 'userId', 'level',
                            'sessionId', 'location', 'userAgent']]
        songplay_data.insert(3, 'songId', songid, True)
        songplay_data.insert(4, 'artistId', artistid, True)
        songplay_data = songplay_data.values[0].tolist()

        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    It reads all the JSON files from the given directory and processes it.

    INPUTS:
    * cur the database cursor
    * conn the database connection
    * path to the folder which should be processed
    * func procedure that will process every file extracted
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb \
                            user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
