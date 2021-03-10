from pg_functions import get_query_create_table, get_query_drop_table, get_query_insert_into
from typing import List


# TableProperties Class Definition

class TableProperties:
    def __init__(self, table_name: str, table_properties: List[tuple]):
        self.table_name = table_name
        self.columns = [table_property[0] for table_property in table_properties]
        self.data_types = [table_property[1] for table_property in table_properties]
        self.validate()
        self.create_statements = self.concat_cols_with_types()
        self.queries = {'create_table': get_query_create_table(self.table_name, *self.create_statements),
                        'drop_table': get_query_drop_table(self.table_name)}

    def validate(self):
        if len(self.columns) != len(self.data_types):
            raise ValueError('Make sure that columns and data_types are equally long.')

    def concat_cols_with_types(self):
        create_statements = [f'{self.columns[index]} {self.data_types[index]}' for index in range(len(self.columns))]
        return create_statements


# TableProperties Instances

songplays_properties = TableProperties('songplays',
                                       [('user_id', 'TEXT PRIMARY KEY'),
                                        ('first_name', 'TEXT'),
                                        ('last_name', 'TEXT'),
                                        ('gender', 'TEXT'),
                                        ('level', 'TEXT')])

users_properties = TableProperties('users',
                                   [('user_id', 'TEXT PRIMARY KEY'),
                                    ('last_name', 'TEXT'),
                                    ('gender', 'TEXT'),
                                    ('level', 'TEXT')])

songs_properties = TableProperties('songs',
                                   [('song_id', 'TEXT PRIMARY KEY'),
                                    ('title', 'TEXT'),
                                    ('artist_id', 'TEXT REFERENCES artists(artist_id)'),
                                    ('year', 'INT'),
                                    ('duration', 'DECIMAL')])

artists_properties = TableProperties('artists',
                                     [('artist_id', 'TEXT PRIMARY KEY'),
                                      ('name', 'TEXT'),
                                      ('location', 'TEXT'),
                                      ('latitude', 'DECIMAL'),
                                      ('longitude', 'DECIMAL')])

time_properties = TableProperties('time',
                                  [('start_time', 'DECIMAL'),
                                   ('hour', 'INT'),
                                   ('day', 'INT'),
                                   ('week', 'INT'),
                                   ('month', 'INT'),
                                   ('year', 'INT'),
                                   ('weekday', 'INT')])

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

songplay_table_insert = get_query_insert_into(songplays_properties.table_name, songplays_properties.columns)
user_table_insert = get_query_insert_into(users_properties.table_name, users_properties.columns)
song_table_insert = get_query_insert_into(songs_properties.table_name, songs_properties.columns)
artist_table_insert = get_query_insert_into(artists_properties.table_name, artists_properties.columns)
time_table_insert = get_query_insert_into(time_properties.table_name, time_properties.columns)

# FIND SONGS

song_select = ("""
""")

# QUERY LISTS

create_table_queries = [artist_table_create, user_table_create, song_table_create,
                        time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
