import snowflake.connector

from scripts import generate_pivot_query as generate


def create_pivot_table(connection_dict, *args, **kwargs):
    """
    Run an SQL command to create a table or a view with the pivoted data
    :param connection_dict: dictionary with connection parameters
    :param args:
    :param kwargs:
    :return: SQL query used to create the table or view, or an error message in case of exceptions
    """
    try:
        connector = snowflake.connector.connect
        with connector(**connection_dict) as connection:
            cursor = connection.cursor()
            destination_schema = kwargs['destination_schema']
            destination_table = kwargs['destination_table']
            pivot_query = generate._generate_pivot_query(cursor, *args, **kwargs)
            # if the destination table name starts with 'TRANSIENT_', a transient table will be created
            stmt = 'CREATE OR REPLACE {TRANSIENT} {OBJECT} {SCHEMA}.{NAME} AS\n{QUERY};' \
                .format(TRANSIENT='TRANSIENT' if destination_table.upper().startswith('TRANSIENT_') else '',
                        OBJECT='VIEW' if kwargs['create_view'] else 'TABLE',
                        SCHEMA=destination_schema,
                        NAME=destination_table,
                        QUERY=pivot_query)
            print(stmt)
            cursor.execute(stmt)
            print('{OBJECT} {NAME} successfully created'.format(OBJECT='View' if kwargs['create_view'] else 'Table',
                                                                NAME=destination_table))
            return stmt
    except Exception as e:
        print(e)
        raise
