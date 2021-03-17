import os
import glob
import sys
from typing import List

import psycopg2
import pandas as pd

from sql_queries import *
from create_tables import drop_tables, create_tables


class ETL:
    def __init__(self, conn, cur):
        self.conn = conn
        self.cur = cur

    @staticmethod
    def get_all_json_files(filepath: str):
        # get all files matching extension from directory
        all_files = []
        for root, dirs, files in os.walk(filepath):
            files = glob.glob(os.path.join(root, '*.json'))
            for f in files:
                all_files.append(os.path.abspath(f))

        # get total number of files found
        num_files = len(all_files)
        print(f'{num_files} files found in {filepath}')

        return all_files

    @staticmethod
    def files_to_df(files: List[str]):
        dfs = []
        for file in files:
            df = pd.read_json(file, lines=True)
            dfs.append(df)
        df = pd.concat(dfs, ignore_index=True)
        return df


def restart(conn, cur):
    drop_tables(cur, conn)
    create_tables(cur, conn)


def set_sys_path():
    """Make sure that sys.path[0] is the directory "src"."""
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


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    conn.autocommit = True

    etl = ETL(conn, cur)

    # Get Song Data
    file_path_song_data = '../data/song_data'
    song_data_files = etl.get_all_json_files(file_path_song_data)
    df_song_data = etl.files_to_df(song_data_files)

    # Get Log Data
    file_path_log_data = '../data/log_data'
    log_data_files = etl.get_all_json_files(file_path_log_data)
    df_log_data = etl.files_to_df(log_data_files)

    # Filter by Pages that have "NextSong" as value
    next_song_pages = df_log_data['page'] == 'NextSong'
    df_log_data = df_log_data[next_song_pages]

    # Upload Artists Data
    artists_data = df_song_data[['artist_id', 'artist_name', 'artist_location',
                                 'artist_latitude', 'artist_longitude']].values

    print(f'Start loading {len(df_song_data.index)} rows for artists table.')
    for row in artists_data:
        cur.execute(artist_table_insert, list(row))
    print('Done')

    # Upload Songs Data
    songs_data = df_song_data[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    print(f'Start loading {len(df_song_data.index)} rows for songs table.')
    for row in songs_data:
        cur.execute(song_table_insert, list(row))
    print('Done')

    # Extract Data for Time Table
    timestamps = df_log_data['ts']
    t = pd.to_datetime(timestamps)
    time_data = [t, t.dt.hour, t.dt.day, t.dt.isocalendar().week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    d = dict(zip(column_labels, time_data))
    df_time = pd.DataFrame(d)

    # Upload Time Data
    print(f'Start loading {len(df_time.index)} rows for time table.')
    for row in df_time.values:
        cur.execute(time_table_insert, list(row))
    print('Done')

    # Upload Users Data
    df_users = df_log_data[['userId', 'firstName', 'lastName', 'gender', 'level']]
    print(f'Start loading {len(df_log_data.index)} rows for users table.')
    for row in df_users.values:
        cur.execute(user_table_insert, list(row))
    print('Done')

    # Upload Songplays Data
    print(f'Start loading {len(df_log_data.index)} rows for songplays table.')
    for index, row in df_log_data.iterrows():

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
        cur.execute(songplay_table_insert, list(songplay_data))
    print('Done')


if __name__ == '__main__':
    set_sys_path()
    # restart()
    main()
