import re
from typing import List


class TableProperties:
    """Class that holds configuration / properties of the tables."""
    def __init__(self, table_name: str, table_properties: List[tuple]):
        """Instantiate an object that holds properties / configuration of a table.

        Args:
            table_name (str): The name of the table, e.g. "songplays".
            table_properties List[tuple]:
              A list of tuples that contain two values. The first value is the column, the second value is the
              configuration for that column. An example of one tuple might be: ('user_id', 'int primary key')
        """
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
        """Check if columns and configuration are equally long."""
        if len(self.columns) != len(self.data_types):
            raise ValueError('Make sure that columns and data_types are equally long.')

    def concat_cols_with_types(self) -> List[str]:
        """Returns a list with strings of statements for "create table" statements in SQL.

        returns List[str]: A list of every create statement for the given table.
        """
        create_statements = [f'{self.columns[index]} {self.data_types[index]}' for index in range(len(self.columns))]
        return create_statements

    def get_query_create_table(self) -> str:
        """Creates the query for "create table"."""
        query_create_table = f"""
        CREATE TABLE IF NOT EXISTS {self.table_name}
          ({', '.join(self.create_statements)});
        """
        query_create_table = query_create_table.replace('\'', '')
        return query_create_table

    def get_query_insert_into(self):
        """Creates the query for "insert into table"."""

        columns = self.columns
        data_types = self.data_types
        primary_key = False
        foreign_keys = []

        for data_type in data_types:

            if re.search('primary key', data_type, re.IGNORECASE):
                index = data_types.index(data_type)
                primary_key = columns[index]

            if re.search('serial', data_type, re.IGNORECASE):
                index = data_types.index(data_type)
                columns.pop(index)

            if re.search('references', data_type, re.IGNORECASE):
                index = data_types.index(data_type)
                foreign_keys.append(columns[index])

        columns_as_string = ', '.join(columns)

        value_placeholders = ''
        for i in enumerate(columns):
            value_placeholders += '(%s), '
        value_placeholders = value_placeholders.strip(', ')

        query = F"""
        INSERT INTO
          {self.table_name} ({columns_as_string})
        VALUES
          ({value_placeholders})
        """

        if primary_key:
            addition = f'ON CONFLICT ({primary_key}) DO NOTHING;'
        else:
            addition = ';'

        return query + addition
