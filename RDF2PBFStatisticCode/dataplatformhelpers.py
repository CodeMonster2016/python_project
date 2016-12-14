import psycopg2
import psycopg2.extras


class PostGresDataBaseConnectionCursor(object):
    """A psycopg2 wrapper to ease connecting and querying a Postgres database in Python"""

    def __init__(self, inputHost, inputDatabase, inputUserName, inputPassward):
        super(PostGresDataBaseConnectionCursor, self).__init__()
        self.host = inputHost
        self.database = inputDatabase
        self.userName = inputUserName
        self.password = inputPassward

        # Instantiate Database Connection
        self.dbConnection = psycopg2.connect(
            'dbname={dbname} host = {host} user={user} password={password}'.format(host=self.host, dbname=self.database,
                                                                                   user=self.userName,
                                                                                   password=self.password))
        self.dbCursor = self.dbConnection.cursor()

    def executeQuery(self, inputQuery):
        self.dbCursor.execute(inputQuery)
        self.dbConnection.commit()

    def retrieveAllQueryResults(self):
        return self.dbCursor.fetchall()

    def close(self):
        self.dbCursor.close()
        self.dbConnection.close()
