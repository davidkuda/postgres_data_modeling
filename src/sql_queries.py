from TableProperties import TableProperties


users_properties = TableProperties('users',
                                   [('user_id', 'INT PRIMARY KEY'),
                                    ('first_name', 'TEXT NOT NULL'),
                                    ('last_name', 'TEXT NOT NULL'),
                                    ('gender', 'TEXT NOT NULL'),
                                    ('level', 'TEXT NOT NULL')])

songs_properties = TableProperties('songs',
                                   [('song_id', 'TEXT PRIMARY KEY'),
                                    ('title', 'TEXT NOT NULL'),
                                    ('artist_id', 'TEXT NOT NULL REFERENCES artists(artist_id)'),
                                    ('year', 'INT NOT NULL'),
                                    ('duration', 'DECIMAL NOT NULL')])

artists_properties = TableProperties('artists',
                                     [('artist_id', 'TEXT PRIMARY KEY'),
                                      ('name', 'TEXT'),
                                      ('location', 'TEXT'),
                                      ('latitude', 'DECIMAL'),
                                      ('longitude', 'DECIMAL')])

time_properties = TableProperties('time',
                                  [('start_time', 'TIMESTAMP PRIMARY KEY'),
                                   ('hour', 'INT NOT NULL'),
                                   ('day', 'INT NOT NULL'),
                                   ('week', 'INT NOT NULL'),
                                   ('month', 'INT NOT NULL'),
                                   ('year', 'INT NOT NULL'),
                                   ('weekday', 'INT NOT NULL')])

songplays_properties = TableProperties('songplays',
                                       [('songplay_id', 'SERIAL PRIMARY KEY'),
                                        ('start_time', 'BIGINT NOT NULL'),
                                        ('user_id', 'INT NOT NULL REFERENCES users(user_id)'),
                                        ('level', 'TEXT NOT NULL'),
                                        ('song_id', 'TEXT REFERENCES songs(song_id)'),
                                        ('artist_id', 'TEXT REFERENCES artists(artist_id)'),
                                        ('session_id', 'TEXT NOT NULL'),
                                        ('location', 'TEXT NOT NULL'),
                                        ('user_agent', 'TEXT NOT NULL')])

# Manually create INSERT INTO statements
users_properties.queries['insert_into'] = """
        INSERT INTO
          users (user_id, first_name, last_name, gender, level)
        VALUES
          ((%s), (%s), (%s), (%s), (%s))
        ON CONFLICT (user_id) 
        DO 
          UPDATE 
            SET 
              first_name = EXCLUDED.first_name,
              last_name = EXCLUDED.last_name,
              level = EXCLUDED.level;
"""

songs_properties.queries['insert_into'] = """
        INSERT INTO
          songs (song_id, title, artist_id, year, duration)
        VALUES
          ((%s), (%s), (%s), (%s), (%s))
        ON CONFLICT (song_id) 
        DO NOTHING;
"""

artists_properties.queries['insert_into'] = """
        INSERT INTO
          artists (artist_id, name, location, latitude, longitude)
        VALUES
          ((%s), (%s), (%s), (%s), (%s))
        ON CONFLICT (artist_id) 
        DO 
          UPDATE
            SET
              name = EXCLUDED.name,
              location = EXCLUDED.location,
              latitude = EXCLUDED.latitude,
              longitude = EXCLUDED.longitude;
"""
time_properties.queries['insert_into'] = """
        INSERT INTO
          time (start_time, hour, day, week, month, year, weekday)
        VALUES
          ((%s), (%s), (%s), (%s), (%s), (%s), (%s))
        ON CONFLICT (start_time) DO NOTHING;
"""
songplays_properties.queries['insert_into'] = """
        INSERT INTO
          songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        VALUES
          ((%s), (%s), (%s), (%s), (%s), (%s), (%s), (%s));
"""


# DROP TABLES

songplay_table_drop = songplays_properties.queries['drop_table']
user_table_drop = users_properties.queries['drop_table']
song_table_drop = songs_properties.queries['drop_table']
artist_table_drop = artists_properties.queries['drop_table']
time_table_drop = time_properties.queries['drop_table']

# CREATE TABLES

songplay_table_create = songplays_properties.queries['create_table']
user_table_create = users_properties.queries['create_table']
song_table_create = songs_properties.queries['create_table']
artist_table_create = artists_properties.queries['create_table']
time_table_create = time_properties.queries['create_table']

# INSERT RECORDS

songplay_table_insert = songplays_properties.queries['insert_into']
user_table_insert = users_properties.queries['insert_into']
song_table_insert = songs_properties.queries['insert_into']
artist_table_insert = artists_properties.queries['insert_into']
time_table_insert = time_properties.queries['insert_into']

# FIND SONGS

song_select = ("""
SELECT song_id, artists.artist_id
FROM songs
JOIN artists
  ON songs.artist_id = artists.artist_id
WHERE
    title = %s
    AND name = %s
    AND duration = %s;
""")

# QUERY LISTS

create_table_queries = [artist_table_create, user_table_create, song_table_create,
                        time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop,
                      artist_table_drop, time_table_drop]


if __name__ == '__main__':
    def print_create_and_drop_queries():
        queries = list()
        queries.extend(create_table_queries)
        queries.extend(drop_table_queries)
        for query in queries:
            print(query)

    def print_queries_from_table_properties(table_properties_objects):
        for o in table_properties_objects:
            print(o.table_name)
            print(o.queries['insert_into'])
            print('')

    # print(songplays_properties.columns)

    postgres_table_properties = [users_properties, songs_properties, artists_properties,
                                 time_properties, songplays_properties]

    for p in postgres_table_properties:
        pass

    print_queries_from_table_properties(postgres_table_properties)