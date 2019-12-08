import psycopg2
from psycopg2 import extras

from framework.configread import ReadConfig


class DbPostgres:

    # this class is for working with Postgres DB
    def __init__(self, config_name=None):

        self.config = config_name
        db_connection = self.read_db_configuration()

        self.host = db_connection['host']
        self.port = int(db_connection['port'])
        self.database = db_connection['database']
        self.user = db_connection['user']
        self.password = db_connection['password']

        self.connection = self.db_connect()
        self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def read_db_configuration(self):
        env_config = ReadConfig(self.config)
        db_connection = env_config.section('PostgreDB')
        return db_connection

    def db_connect(self):
        connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
        )

        connection.autocommit = False

        return connection

    def safe_execute(self, sql):

        """
        this function executes any rubbish and rolls back changes if there is syntax's error in query
        :param sql: request to execute
        :return: result for 'select'
        """

        try:
            self.cursor.execute(sql)
        except Exception as e:
            self.connection.rollback()
            raise e

        if sql.lower().startswith('select') or sql.lower().startswith('with'):
            res = self.cursor.fetchall()
        else:
            res = self.cursor.rowcount
            self.connection.commit()

        return res

    def __del__(self):
        self.connection.close()
