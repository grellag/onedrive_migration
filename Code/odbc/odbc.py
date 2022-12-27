import pyodbc

def createDBConnection(server, database, user, password):
    connection = pyodbc.connect(Driver="{ODBC Driver 17 for SQL Server}",
                                Server=server,
                                Database=database,
                                UID=user,
                                PWD=password)
    return connection

def closeDBConnection(connection):
    try:
        connection.close()
    except pyodbc.ProgrammingError:
        pass