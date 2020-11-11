import snowflake.connector

transient_table_query = """
SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_NAME LIKE 'TRANSIENT\_{source}\_%'
  AND TABLE_TYPE = 'BASE TABLE'
  AND IS_TRANSIENT = 'YES'
"""


def drop_transient_tables(connection_dict, source, *args, **kwargs):
    """
    Drop transient tables having names starting with 'TRANSIENT_' and containing the specified source string
    :param connection_dict: dictionary with connection parameters
    :param source: source of the data (typical values: 'AE', 'AT', 'XS')
    :param args:
    :param kwargs:
    """
    try:
        connector = snowflake.connector.connect
        with connector(**connection_dict) as connection:
            cursor = connection.cursor()
            query = transient_table_query.format(source=source)
            cursor.execute(query)
            rows = list(cursor)
            for row in rows:
                stmt = 'DROP TABLE {SCHEMA}.{TABLE};'.format(SCHEMA=row[0], TABLE=row[1])
                print(stmt)
                cursor.execute(stmt)
                print('Table {SCHEMA}.{TABLE} successfully dropped'.format(SCHEMA=row[0], TABLE=row[1]))
    except Exception as e:
        print(e)
        raise
