import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    # open song file
    df_song_file = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = df_song_file[['song_id', 'title', 'artist_id', 'year', 'duration']].values
    for record in song_data:
        record_as_list = list(record)
        cur.execute(song_table_insert, record_as_list)
    
    # insert artist record
    artist_data = df_song_file[['artist_id', 'artist_name', 'artist_location',
                                'artist_latitude', 'artist_longitude']].values
    for record in artist_data:
        record_as_list = list(record)
        cur.execute(artist_table_insert, record_as_list)


def process_log_file(cur, filepath):
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
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = 
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, file_processor):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print(f'{num_files} files found in {filepath}')

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        file_processor(cur, datafile)
        conn.commit()
        print(f'{i}/{num_files} files processed.')


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', file_processor=process_song_file)
    process_data(cur, conn, filepath='data/log_data', file_processor=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()