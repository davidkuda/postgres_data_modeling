def get_query_drop_table(table_name):
    return f'DROP TABLE IF EXISTS {table_name};'


def get_query_create_table(table_name, *args):
    query_create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name}
      {args};
    """
    query_create_table = query_create_table.replace('\'', '')
    return query_create_table


def get_query_insert_into(table_name, columns):
    
    if ['SERIAL', 'serial'] in columns:
        columns.remove()

    columns_as_string = ', '.join(columns)

    value_placeholders = ''
    for i in columns:
        value_placeholders += '(%s), '
    value_placeholders = value_placeholders.strip(', ')

    query = F"""
    INSERT INTO
      {table_name} ({columns_as_string})
    VALUES
      ({value_placeholders});
    """
    return query
