import os
import glob
import sys
from typing import List, Callable

import psycopg2
import pandas as pd

from sql_queries import *
from create_tables import drop_tables, create_tables


def set_sys_path():
    """Change "sys.path[0]" according to the requirements.

    Make sure that sys.path[0] is the directory "src"."""
    current_path = sys.path[0]
    print('Current Path:')
    print(current_path)
    if current_path.endswith('/src'):
        print('Your path is correct')
        return
    # Correct path if necessary
    correct_path = os.path.abspath('src/')
    print(f'Setting path to "{correct_path}"')
    sys.path.insert(0, correct_path)


def process_song_file(cur, filepath: List[str]) -> None:
    """Processes song files and uploads data to the tables artists and songs.

    Args:
        cur: The cursor from the database connection with psycopg2.
        filepath (List[str]):
          A list of strings where each string represents the path to the file
          that contains the data.

    Returns:
        None
    """
    # open song file
    df_song_file = pd.read_json(filepath, lines=True)

    # insert artist record
    artist_data = df_song_file[['artist_id', 'artist_name', 'artist_location',
                                'artist_latitude', 'artist_longitude']].values
    for record in artist_data:
        record_as_list = list(record)
        cur.execute(artist_table_insert, record_as_list)

    # insert song record
    song_data = df_song_file[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    for record in song_data:
        record_as_list = list(record)
        cur.execute(song_table_insert, record_as_list)


def process_log_file(cur, filepath: List[str]):
    """Processes log files and uploads data to the tables users and time.

        Args:
            cur: The cursor from the database connection with psycopg2.
            filepath (List[str]):
              A list of strings where each string represents the path to the file
              that contains the data.

        Returns:
            None
        """
    # open log file
    df_log_file = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    next_song_pages = df_log_file['page'] == 'NextSong'
    df_log_file = df_log_file[next_song_pages]

    # convert timestamp column to datetime
    timestamp = df_log_file['ts']
    t = pd.to_datetime(timestamp)

    # insert time data records
    time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    d = dict(zip(column_labels, time_data))
    df_time = pd.DataFrame(d)

    for i, row in df_time.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    df_users = df_log_file[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in df_users.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df_log_file.iterrows():

        # get song_id and artist_id from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            song_id, artist_id = results
        else:
            song_id, artist_id = None, None

        # insert songplay record
        ids = pd.Series([song_id, artist_id], index=['song_id', 'artist_id'])
        enriched_row = row.append(ids)
        songplay_data = enriched_row[['ts', 'userId', 'level', 'song_id', 'artist_id',
                                      'sessionId', 'location', 'userAgent']]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath: str, file_processor: Callable) -> None:
    """Collects all json files from a directory and uses the file_processor on them.

    Args:
        cur: The cursor from the database connection with psycopg2.
        conn: The database connection with psycopg2.
        filepath (str): The path to the directory that contains the files.
        file_processor (Callable):
          Either the  function "process_song_files" or "process_log_files".

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root, '*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        file_processor(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed.')


def restart() -> None:
    """Drops all tables and creates all tables again (without data)."""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    conn.close()


def main():
    """Connects to db and processes song_data and log_data"""
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', file_processor=process_song_file)
    process_data(cur, conn, filepath='data/log_data', file_processor=process_log_file)

    conn.close()


if __name__ == "__main__":
    # set_sys_path()
    restart()
    main()
