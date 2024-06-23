import pymysql

# Database connection details
rds_host = "kakepa-final-sql-db.cng0we28yzs1.us-east-1.rds.amazonaws.com"
rds_user = "admin"
rds_password = "DolphinDB11!"


# Connect to the database
def connect_to_db(database=None):
    try:
        connection = pymysql.connect(
            host=rds_host,
            user=rds_user,
            password=rds_password,
            database=database,
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Connection successful")
        return connection

    except pymysql.MySQLError as e:
        print(f"Error connecting to MySQL Platform: {e}")
        return None


# Get names of the database instances and tables in it
def get_db_and_table_names():
    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute("SHOW DATABASES;")
                databases = cursor.fetchall()
                db_and_tables = {}
                for db in databases:
                    db_name = db['Database']
                    db_and_tables[db_name] = []
                    connection.select_db(db_name)
                    cursor.execute("SHOW TABLES;")
                    tables = cursor.fetchall()
                    for table in tables:
                        table_name = list(table.values())[0]
                        db_and_tables[db_name].append(table_name)
                return db_and_tables
        except pymysql.MySQLError as e:
            print(f"Error fetching databases and tables: {e}")
            return None
        finally:
            connection.close()


def print_dbs_and_tables(db_name=None):
    db_and_tables = get_db_and_table_names()
    if db_and_tables:
        if db_name:
            if db_name in db_and_tables:
                print(f"Database: {db_name}")
                for table in db_and_tables[db_name]:
                    print(f"  Table: {table}")
            else:
                print(f"Database '{db_name}' does not exist.")
        else:
            for db, tables in db_and_tables.items():
                print(f"Database: {db}")
                for table in tables:
                    print(f"  Table: {table}")
    else:
        print('No databases found.')


# works
def create_database(db_name):
    connection = connect_to_db()
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
                connection.commit()
                print(f"Database '{db_name}' created successfully.")
        except pymysql.MySQLError as e:
            print(f"Error creating database: {e}")
        finally:
            connection.close()


# works
def create_table(db_name, table_name, columns):
    connection = connect_to_db(db_name)
    if connection:
        try:
            with connection.cursor() as cursor:
                column_definitions = ", ".join([f"{col_name} {data_type}" for col_name, data_type in columns.items()])
                create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_definitions});"
                cursor.execute(create_table_query)
                connection.commit()
                print(f"Table '{table_name}' created successfully in database '{db_name}'.")
        except pymysql.MySQLError as e:
            print(f"Error creating table: {e}")
        finally:
            connection.close()


def delete_table(db_name, table_name):
    connection = connect_to_db()  # Connect to MySQL without selecting a specific database
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"USE {db_name};")  # Select the database to operate within
                cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
                connection.commit()
                print(f"Table '{table_name}' deleted successfully from database '{db_name}'.")
        except pymysql.MySQLError as e:
            print(f"Error deleting table: {e}")
        finally:
            connection.close()


if __name__ == "__main__":
    # new_table_columns = {
    #     "id": "INT AUTO_INCREMENT PRIMARY KEY",
    #     "name": "VARCHAR(255) NOT NULL",
    #     "email": "VARCHAR(255) NOT NULL UNIQUE",
    #     "age": "INT"
    # }
    #
    # create_database('DBTest1')
    #
    # create_table('DBTest1', "Test1Table1", new_table_columns)

    print_dbs_and_tables()
