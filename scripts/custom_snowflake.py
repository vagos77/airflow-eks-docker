import logging
import snowflake.connector


class SnowflakeDatabase(object):

    def __init__(self, connection_dict):
        """
        Initialise Snowflake connection
        :param connection_dict: dictionary with connection parameters
        """
        self.logger = logging.getLogger(__name__ + '.Database')
        self.connection_dict = connection_dict

    def get_connection(self):
        """
        Returns a database connection object
        """
        try:
            self.logger.debug(self.connection_dict)
            connection = snowflake.connector.connect(**self.connection_dict)
        except Exception as e:
            self.logger.error(e)
            raise e
        return connection

    def get_cursor(self, connection):
        """
        Returns a database cursor object
        :param connection: a Snowflake connection
        """
        try:
            cursor = connection.cursor()
        except Exception as e:
            self.logger.error(e)
            raise e
        return cursor

    def test_connection(self):
        """
        Test connection to Snowflake
        :return: the version of the Snowflake database
        """
        version = 'Not connected'
        conn = self.get_connection()
        cs = self.get_cursor(conn)
        try:
            cs.execute("SELECT current_version()")
            one_row = cs.fetchone()
            version = one_row[0]
        except Exception as e:
            print(e)
            self.logger.error(e)
        finally:
            cs.close()
            conn.close()
        print('Snowflake version: {}'.format(version))
        return version

    def execute_query(self, sql, return_results=True):

        """
        Test connection to Snowflake
        :return: the version of the Snowflake database
        """
        conn = self.get_connection()
        cs = self.get_cursor(conn)
        try:
            cs.execute(sql)

            results = cs.fetchall() if return_results else []
            return results

        except Exception as e:
            print(e)
            self.logger.error(e)
        finally:
            cs.close()
            conn.close()
