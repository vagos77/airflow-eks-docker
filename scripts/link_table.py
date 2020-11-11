import io
import logging
# import os
# import sys

# currentdir = os.path.dirname(os.path.realpath(__file__))
# parentdir = os.path.dirname(currentdir)
# sys.path.append(parentdir)

from scripts import custom_snowflake as db_sf

logger = logging.getLogger(__name__)


def generate_link_table_query(snowflake_connection, database):

    get_pk_fk_columns = """
    SELECT 
        TABLE_NAME,
        LISTAGG(COLUMN_NAME, ',') within group (order by ORDINAL_POSITION) AS LINK_TABLE_COLUMNS
    FROM {DATABASE}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA='BUSINESS_VAULT' 
        AND (COLUMN_NAME LIKE 'FK_%' OR COLUMN_NAME LIKE 'PK_%')
        AND TABLE_NAME LIKE 'FACT_%'
    GROUP BY TABLE_NAME
    """.format(DATABASE=database)

    # Dictionary that will hold the table name of each FACT table and the PK/FK columns
    fact_tables_with_link_cols = {}
    all_unique_link_table_cols = []

    print('Gathering PKs/FKs')
    for row in snowflake_connection.execute_query(get_pk_fk_columns):
        table = row[0]
        cols = row[1].split(',')

        # TODO Add logger DEBUG
        # print('Table: {} \n Cols: {}'.format(table, cols))

        fact_tables_with_link_cols[table] = cols

        for c in cols:
            if c not in all_unique_link_table_cols:
                # TODO Add logger Debug
                # print('Adding {} to Link Table'.format(c))
                # Columns in theLink Table only use PK_ thus FK will be renamed before we add them to the list
                all_unique_link_table_cols.append(c)

    # print(fact_tables_with_link_cols)
    link_table_query = io.StringIO()

    link_table_query.write('CREATE OR REPLACE TABLE {DATABASE}.BUSINESS_VAULT.LINK_TABLE AS ('.format(DATABASE=database))

    for t, cols in fact_tables_with_link_cols.items():
        # print('Table is: ', t)
        # print('Table has cols: ', fact_tables_with_link_cols[t])

        # After the first SELECT start using UNION
        if 'SELECT' in link_table_query.getvalue():
            link_table_query.write('UNION\n')

        # Write SELECT statement
        link_table_query.write('SELECT\n')

        # For each column in the list of unique columns check if it exists in the table we're processing.
        for i, unique_col in enumerate(all_unique_link_table_cols):

            # Add comma at the beginning of each col for all cols apart from the first one
            if i > 0:
                link_table_query.write(',')

            # Write column name
            column_exists_in_table = True if unique_col in fact_tables_with_link_cols[t] else False
            if column_exists_in_table:
                # Unique column exists in the table. For values use what's there, replace FK_ references to match the field
                # used when joining to Dimensions
                col_name = '{} AS {}\n'.format(unique_col, unique_col.replace('FK_', 'PK_')) if 'FK_' in unique_col \
                    else '{}\n'.format(unique_col)
                link_table_query.write(col_name)
            else:
                # Unique column doesnt exist in the table. For values use MD5(''), replace FK_ references to match the field
                # used when joining to Dimensions
                link_table_query.write('MD5(\'\') AS {}\n'.format(unique_col.replace('FK_', 'PK_')))
        # Add LinkSource
        link_table_query.write(',\'{}\' AS LinkSource\n'.format(t))

        # Add FROM statement
        link_table_query.write('FROM {}.BUSINESS_VAULT.{}\n'.format(database, t))

    # Close create table statement
    link_table_query.write(');')

    return link_table_query.getvalue()


def main():
    snowflake_dict = {
        'account': 'altusgroupargus.eu-west-1',
        'user': 'EPERTSINIS',
        'password': 'dkpVEb9J&Z*v82tD',
        'warehouse': 'LOAD_WH_VAGOS'
    }

    database = 'AIRFLOWSEQUENTIAL'

    # Init Snowflake object
    snowflake_connection = db_sf.SnowflakeDatabase(snowflake_dict)

    link_table_sql = generate_link_table_query(snowflake_connection, database)

    print(link_table_sql)

    # Execute query
    print('Executing query ...')
    snowflake_connection.execute_query(link_table_sql)


if __name__ == "__main__":
    main()
