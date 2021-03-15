import re
from typing import List

from pg_functions import get_query_create_table, get_query_drop_table, get_query_insert_into


# TableProperties Class Definition

class TableProperties:
    def __init__(self, table_name: str, table_properties: List[tuple]):
        self.table_name = table_name
        self.columns = [table_property[0] for table_property in table_properties]
        self.data_types = [table_property[1] for table_property in table_properties]
        self.validate()
        self.create_statements = self.concat_cols_with_types()
        self.queries = {'create_table': self.get_query_create_table(),
                        'drop_table': f'DROP TABLE IF EXISTS {self.table_name};',
                        'insert_into': self.get_query_insert_into(),
                        'select': f'SELECT {", ".join(self.columns)} FROM {self.table_name};'}

    def validate(self):
        if len(self.columns) != len(self.data_types):
            raise ValueError('Make sure that columns and data_types are equally long.')

    def concat_cols_with_types(self):
        create_statements = [f'{self.columns[index]} {self.data_types[index]}' for index in range(len(self.columns))]
        return create_statements

    def get_query_create_table(self):
        query_create_table = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name}
          ({', '.join(self.create_statements)});
        """
        query_create_table = query_create_table.replace('\'', '')
        return query_create_table

    def get_query_insert_into(self):

        columns = self.columns
        data_types = self.data_types

        for data_type in data_types:
            if re.search('serial', data_type, re.IGNORECASE):
                index = data_types.index(data_type)
                columns.pop(index)

        columns_as_string = ', '.join(columns)

        value_placeholders = ''
        for i in enumerate(columns):
            value_placeholders += '(%s), '
        value_placeholders = value_placeholders.strip(', ')

        query = F"""
        INSERT INTO
          {self.table_name} ({columns_as_string})
        VALUES
          ({value_placeholders});
        """
        return query


# TableProperties Instances

users_properties = TableProperties('users',
                                   # [('user_id', 'TEXT PRIMARY KEY'),
                                   [('user_id', 'TEXT'),
                                    ('first_name', 'TEXT'),
                                    ('last_name', 'TEXT'),
                                    ('gender', 'TEXT'),
                                    ('level', 'TEXT')])

songs_properties = TableProperties('songs',
                                   # [('song_id', 'TEXT PRIMARY KEY'),
                                   [('song_id', 'TEXT'),
                                    ('title', 'TEXT'),
                                    # ('artist_id', 'TEXT REFERENCES artists(artist_id)'),
                                    ('artist_id', 'TEXT'),
                                    ('year', 'INT'),
                                    ('duration', 'DECIMAL')])

artists_properties = TableProperties('artists',
                                     # [('artist_id', 'TEXT PRIMARY KEY'),
                                     [('artist_id', 'TEXT'),
                                      ('name', 'TEXT'),
                                      ('location', 'TEXT'),
                                      ('latitude', 'DECIMAL'),
                                      ('longitude', 'DECIMAL')])

time_properties = TableProperties('time',
                                  [('start_time', 'TIMESTAMP'),
                                   ('hour', 'INT'),
                                   ('day', 'INT'),
                                   ('week', 'INT'),
                                   ('month', 'INT'),
                                   ('year', 'INT'),
                                   ('weekday', 'INT')])

songplays_properties = TableProperties('songplays',
                                       [('songplay_id', 'SERIAL PRIMARY KEY'),
                                        ('start_time', 'TIMESTAMP'),
                                        # ('user_id', 'TEXT REFERENCES users(user_id)'),
                                        ('user_id', 'TEXT'),
                                        ('level', 'TEXT'),
                                        # ('song_id', 'TEXT REFERENCES songs(song_id)'),
                                        ('song_id', 'TEXT'),
                                        # ('artist_id', 'TEXT REFERENCES artists(id)'),
                                        ('artist_id', 'TEXT'),
                                        ('session_id', 'TEXT'),
                                        ('location', 'TEXT'),
                                        ('user_agent', 'TEXT')])

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
SELECT artists.artist_id, song_id
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
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

if __name__ == '__main__':
    def print_queries():
        queries = list()
        queries.extend(create_table_queries)
        queries.extend(drop_table_queries)
        for query in queries:
            print(query)

    # print(songplays_properties.columns)

    postgres_table_properties = [users_properties, songs_properties, artists_properties,
                                 time_properties, songplays_properties]

    for p in postgres_table_properties:
        pass

    print(songplays_properties.queries['insert_into'])
