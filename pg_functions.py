def get_query_drop_table(table_name):
    return f'DROP TABLE IF EXISTS {table_name};'


def get_query_create_table(table_name, *args):
    query_create_table = f"""
    CREATE TABLE IF NOT EXISTS {table_name}
      {args};
    """
    query_create_table = query_create_table.replace('\'', '')
    return query_create_table


def get_query_insert_into(table_name, **kwargs):
    columns = ', '.join(kwargs.keys())
    values_list = (kwargs.values())
    values_tuple = tuple(values_list)

    value_placeholders = ''
    for value in values_list:
        value_placeholders += '(%s), '
    value_placeholders = value_placeholders.strip(', ')

    query = F"""
    INSERT INTO
      {table_name}({columns})
    VALUES
      ({value_placeholders});
    """
    return query, values_tuple