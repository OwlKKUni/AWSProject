import os
import pymysql
import time


class DBConnString:
    def __init__(self, server, database, username, password, port=3306):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.port = port
        self.cursorclass = pymysql.cursors.DictCursor


class SQLQuery:
    def __init__(self, table_name, **kwargs):
        self.table_name = table_name
        self.columns = kwargs

    def add_column(self, column_name, column_type):
        self.columns[column_name] = column_type

    def generate_query(self):
        query = f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
        column_definitions = [f"id INT AUTO_INCREMENT PRIMARY KEY"]  # first column
        for column_name, column_type in self.columns.items():
            column_definitions.append(f"{column_name} {column_type}")
        query += ", ".join(column_definitions)
        query += ");"
        return query


# WORKS
def connect(conn_string: DBConnString):
    try:
        conn = pymysql.connect(
            host=conn_string.server,
            database=conn_string.database,
            user=conn_string.username,
            password=conn_string.password,
            port=conn_string.port,
            cursorclass=conn_string.cursorclass
        )
        return conn

    except pymysql.connect.Error as e:
        print(f'Error in connect()')
        print(f"Error connecting to the database: {e}")
        return None


# CONN STRING FOR SERVERS
Server1 = DBConnString(
    os.environ['AWS_RDS_ENDPOINT'],
    os.environ['AWS_RDS_DATABASE'],
    os.environ['AWS_RDS_USERNAME'],
    os.environ['AWS_RDS_PASSWORD'],
)

# QUERIES FOR CREATING EMPTY TABLES
tquery_objectives = SQLQuery(table_name='objectives_completed',
                             main_objectives='INT',
                             optional_objectives='INT',
                             helldivers_extracted='INT',
                             outposts_destroyed_light='INT',
                             outposts_destroyed_medium='INT',
                             outposts_destroyed_heavy='INT',
                             mission_time_remaining='TIME'
                             ).generate_query()

tquery_samples = SQLQuery(table_name='samples_gained',
                          green_samples='INT',
                          orange_samples='INT',
                          violet_samples='INT'
                          ).generate_query()

tquery_currency = SQLQuery(table_name='currency_gained',
                           requisition='INT',
                           medals='INT',
                           xp='INT'
                           ).generate_query()

tquery_combat = SQLQuery(table_name='combat',
                         kills='INT',
                         accuracy="DECIMAL(5,2)",
                         shots_fired='INT',
                         deaths='INT',
                         stims_used='INT',
                         accidentals='INT',
                         samples_extracted='INT',
                         stratagems_used='INT',
                         melee_kills='INT',
                         times_reinforcing='INT',
                         friendly_fire_damage='INT',
                         distance_travelled='INT',
                         ).generate_query()


# CREATE TABLES
# ----------------------------
# WORKS

# OLD
# def query_create_tables(server_name: DBConnString, table_queries: list) -> None:
#     connection = connect(server_name)
#     if connection:
#         try:
#             with connection.cursor() as cursor:
#                 for table_query in table_queries:
#                     cursor.execute(table_query)
#                     print(f'Table "{table_query.split(" ")[2]}" created')
#
#                 connection.commit()
#         except pymysql.MySQLError as e:
#             print(f'Error in query_create_tables()')
#             print(f"Error creating tables: {e}")
#         finally:
#             connection.close()

# NEW
def query_create_tables(server_name: DBConnString, db_name: str, table_queries: list) -> None:
    connection = connect(server_name)
    if connection:
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"USE {db_name}")  # Switch to the target database
                for table_query in table_queries:
                    cursor.execute(table_query)
                    print(f'Table "{table_query.split(" ")[2]}" created')
                connection.commit()
        except pymysql.MySQLError as e:
            print(f'Error in query_create_tables()')
            print(f"Error creating tables in database '{db_name}': {e}")
        finally:
            connection.close()


# READ TABLES
# ----------------------------
# WORKS
def query_read_row(server_name: DBConnString, table: str, row_id: int) -> None:
    sql_query = f"SELECT * FROM {table} WHERE id = %s"
    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query, (row_id,))
                row = cursor.fetchone()
                if row:
                    print(row)
                else:
                    print(f"No row with id {row_id} found in table '{table}'")
        else:
            print("Connection to database failed.")
    except pymysql.MySQLError as e:
        print(f'Error in query_read_row()')
        print(f"Error reading row from table '{table}': {e}")


# OUTPUT OF IT
# Connection successful
# {'id': 3, 'requisition': 1, 'medals': 1, 'xp': 2137}
# {'id': 4, 'requisition': 1, 'medals': 1, 'xp': 2137}

# WORKS
def query_read_table(server_name: DBConnString, table: str) -> None:
    sql_query = f"SELECT * FROM {table}"
    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                rows = cursor.fetchall()
                if rows:
                    for row in rows:
                        print(row)
                else:
                    print(f"No data found in table '{table}'")
        else:
            print("Connection to database failed.")
    except pymysql.MySQLError as e:
        print(f'Error in query_read_table()')
        print(f"Error reading table '{table}': {e}")


# GET TABLES
# ----------------------------
# WORKS
def query_get_table_names(server_name: DBConnString):
    # Stupid fucking fix that takes into account 156 tables created on database creation - OH WELL...
    # I AM SO FUCKING TIRED MAN...
    sql_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' LIMIT 156, 1000000"

    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query)
            table_names = [row['TABLE_NAME'] for row in cursor.fetchall()]

        if table_names:
            return table_names

        else:
            print("No data found.")
            return []

    except pymysql.MySQLError as e:
        print(f'Error in query_get_table_names()')
        print(f"Error executing SQL query: {e}")
        return []


# WORKS
def query_get_table_column_names(server_name: DBConnString, table: str) -> list:
    data = []
    with connect(server_name).cursor() as cursor:
        cursor.execute(f'SELECT * FROM {table}')
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        data.append(columns)
        for row in rows:
            data.append(list(row))
    return data


# WORKS
def query_get_data_from_table(server_name: DBConnString, table: str) -> list:
    with connect(server_name).cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table}")
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        data = [columns] + [list(row.values()) for row in rows]  # Convert dictionary values to list
        return data


# UPDATE
# ----------------------------
# Works
# Updates by column "id" not global id

def query_update_cell(server_name: DBConnString, table_name: str, column_name: str, id_: int, value: any) -> None:
    sql_query = f"UPDATE {table_name} SET {column_name} = %s WHERE id = %s"

    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query, (value, id_))
            connection.commit()  # Commit outside the cursor context
            print(f'Table "{table_name}" updated')
        else:
            print("Connection to database failed.")

    except pymysql.MySQLError as e:
        print(f'Error in query_update_cell()')
        print(f"Error updating table '{table_name}': {e}")
    except Exception as e:
        print(f'Error in query_update_cell()')
        print(f"An unexpected error occurred: {e}")


# Works
def query_update_row(server_name: DBConnString, table_name: str, id_: int, data: dict) -> None:
    for column_name, value in data.items():
        query_update_cell(server_name, table_name, column_name, id_, value)


# DELETE
# --------------------------------
# WORKS
def query_delete_all_tables(server_name: DBConnString):
    try:
        table_names = query_get_table_names(server_name)
        if table_names:
            for table_name in table_names:
                query_delete_table(server_name, table_name)
        else:
            print("No data found to delete.")

    except Exception as e:
        print(f'Error in query_delete_all_tables()')
        print(f"An unexpected error occurred: {e}")


# WORKS
def query_delete_table(server_name: DBConnString, table_name: str) -> None:
    sql_query = f"DROP TABLE IF EXISTS {table_name}"
    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query)
            print(f'Table "{table_name}" deleted')
        connect(server_name).commit()
    except pymysql.MySQLError as e:
        print(f'Error in query_delete_table()')
        print(f"Error deleting table '{table_name}': {e}")
    finally:
        connect(server_name).close()


# W O R K S !!!
def query_delete_row(server_name: DBConnString, table_name: str, id_value: int) -> None:
    sql_query = f"DELETE FROM {table_name} WHERE id = %s"

    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query, (id_value,))
            connection.commit()
            print(f"Row with ID {id_value} deleted from table '{table_name}'")
        else:
            print("Connection to database failed.")
    except pymysql.MySQLError as e:
        print(f'Error in query_delete_row()')
        print(f"Error deleting row from table '{table_name}': {e}")
    except Exception as e:
        print(f'Error in query_delete_row()')
        print(f"An unexpected error occurred: {e}")
    finally:
        if connection:
            connection.close()


# PUT
# ----------------------------------------------
# WORKS
# Example call: query_put_row(Server1, 'currency_gained', requisition=2, medals=1, xp=2140) -> id is auto assigned
def query_put_row(server_name: DBConnString, table_name: str, **kwargs) -> None:
    columns = ', '.join(kwargs.keys())
    values_placeholders = ', '.join(['%s'] * len(kwargs))
    values = tuple(kwargs.values())

    sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholders})"

    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query, values)
            connection.commit()
            print(f"Row inserted into table '{table_name}'")
        else:
            print("Connection to database failed.")

    except pymysql.MySQLError as e:
        print(f'Error in query_put_row()')
        print(f"Error inserting row into table '{table_name}': {e}")
    except Exception as e:
        print(f'Error in query_put_row()')
        print(f"An unexpected error occurred: {e}")


# Aux functions
# ------------------------
# WORKS - returns the non-system ID
def query_get_last_id_value(server_name: DBConnString, table_name: str) -> int:
    sql_query = f"SELECT id FROM {table_name} ORDER BY id DESC LIMIT 1"
    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchone()
            return result[
                'id'] if result else -1  # Access the result using the column name ----THIS WAS NONE BEFORE - NOT -1

    except pymysql.MySQLError as e:
        print('query_get_last_id_value()')
        print(f"Error occurred: {e}")
        return None


# W O R K S

# dict {columns: [], rows: [{},{}]}
# I GUESS??? RETURNS WHAT'S ABOVE ANYWAY
def query_get_data_by_id(server_name: DBConnString, table: str, id_value: int) -> dict:
    data = {
        "columns": [],
        "rows": []
    }
    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(f'SELECT * FROM {table} WHERE id = %s', (id_value,))
                rows = cursor.fetchall()

                # Check if rows were returned
                if rows:
                    columns = [desc[0] for desc in cursor.description if desc[0] != 'id']
                    data["columns"] = columns
                    data["rows"] = [
                        [value for desc, value in zip(cursor.description, row) if desc[0] != 'id']
                        for row in rows
                    ]
                else:
                    print(f"No rows found in table '{table}' with id = {id_value}")

    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection:
            connection.close()

    return data


# Additions --------------------------------------
def query_check_db_exists(server_name: DBConnString, db_name: str) -> bool:
    sql_query = f"SHOW DATABASES LIKE '{db_name}'"
    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                result = cursor.fetchone()
                print(bool(result))
                return bool(result)
        else:
            print("Connection to database failed.")
            return False
    except pymysql.MySQLError as e:
        print(f'Error in query_check_db_exists()')
        print(f"Error checking if database exists: {e}")
        return False
    finally:
        if connection:
            connection.close()


def query_check_table_exists(server_name: DBConnString, db_name: str, table_name: str) -> bool:
    sql_query = f"SHOW TABLES FROM {db_name} LIKE '{table_name}'"
    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                result = cursor.fetchone()
                return bool(result)
        else:
            print("Connection to database failed.")
            return False
    except pymysql.MySQLError as e:
        print(f'Error in query_check_table_exists()')
        print(f"Error checking if table '{table_name}' exists in database '{db_name}': {e}")
        return False
    finally:
        if connection:
            connection.close()


def query_create_db(server_name: DBConnString, db_name: str) -> None:
    sql_query = f"CREATE DATABASE IF NOT EXISTS {db_name}"
    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
            connection.commit()
            print(f"Database '{db_name}' created")
        else:
            print("Connection to database failed.")
    except pymysql.MySQLError as e:
        print(f'Error in query_create_db()')
        print(f"Error creating database '{db_name}': {e}")
    finally:
        if connection:
            connection.close()


def query_delete_db(server_name: DBConnString, db_name: str) -> None:
    sql_query = f"DROP DATABASE IF EXISTS {db_name}"
    try:
        connection = connect(server_name)
        if connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_query)
                print(f"Database '{db_name}' deleted")
            connection.commit()  # Commit the change
        else:
            print("Connection to database failed.")
    except pymysql.MySQLError as e:
        print(f'Error in query_delete_db()')
        print(f"Error deleting database '{db_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred while deleting database '{db_name}': {e}")
    finally:
        if connection:
            connection.close()


def create_table_if_not_exists(connection, table_name, table_creation_query):
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            table_exists = cursor.fetchone()

            if not table_exists:
                print(f"Table '{table_name}' does not exist. Creating...")
                cursor.execute(table_creation_query)
                connection.commit()  # Commit table creation
                print(f"Table '{table_name}' created.")
            else:
                print(f"Table '{table_name}' already exists.")
    except pymysql.MySQLError as e:
        print(f"Error creating table '{table_name}': {e}")


def setup_db_and_tables(server_name: DBConnString):
    db_name = 'DBTest1'
    db_created = False
    db_exists = False  # Initialize db_exists variable
    max_wait_time = 30  # Wait up to 30 seconds for the database creation
    wait_time_interval = 5  # Wait 5 seconds between checks

    # Step 1: Try connecting to the MySQL server without a database (i.e., database=None)
    connection = connect(DBConnString(
        server_name.server,  # Same server
        '',  # No database specified initially
        server_name.username,
        server_name.password,
        server_name.port
    ))  # Use the connection without a database

    if connection:
        try:
            # Step 2: If connection successful, move to database existence check and creation
            with connection.cursor() as cursor:
                cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
                db_exists = cursor.fetchone()

                # If the database doesn't exist, create it
                if not db_exists:
                    print(f"Database '{db_name}' does not exist. Creating...")
                    cursor.execute(f"CREATE DATABASE {db_name}")
                    connection.commit()  # Commit the database creation
                    print(f"Database '{db_name}' creation initiated.")
                    connection.close()  # Close the initial connection

                    # Wait for the database to be created and check periodically
                    start_time = time.time()
                    while time.time() - start_time < max_wait_time:
                        # Reconnect to the MySQL server without selecting a database
                        connection = connect(server_name)
                        if connection:
                            with connection.cursor() as cursor:
                                cursor.execute(f"SHOW DATABASES LIKE '{db_name}'")
                                db_exists = cursor.fetchone()
                                if db_exists:
                                    db_created = True
                                    print(f"Database '{db_name}' successfully created.")
                                    break  # Exit the loop once the database is created
                        if not db_created:
                            print(f"Waiting for database '{db_name}' to be created...")
                            time.sleep(wait_time_interval)  # Wait before checking again

                    # If the database is not created within the time limit, raise an error
                    if not db_created:
                        raise Exception(f"Database '{db_name}' could not be created within {max_wait_time} seconds.")

                else:
                    print(f"Database '{db_name}' already exists.")

        except pymysql.MySQLError as e:
            print(f"Error in setup_db_and_tables(): {e}")
        finally:
            # Close the connection after performing the database check/creation
            connection.close()

        # Step 3: If the database was created or already exists, proceed to create tables
        if db_created or db_exists:
            # Reconnect to the specific database DBTest1
            connection = connect(DBConnString(
                server_name.server,  # Same server
                db_name,  # The created or already existing database
                server_name.username,
                server_name.password,
                server_name.port
            ))  # Now connect with the DB

            if connection:
                try:
                    connection.select_db(db_name)  # Select DBTest1 after connection

                    # Step 4: Create tables if they don't exist
                    create_table_if_not_exists(connection, 'objectives_completed', tquery_objectives)
                    create_table_if_not_exists(connection, 'samples_gained', tquery_samples)
                    create_table_if_not_exists(connection, 'currency_gained', tquery_currency)
                    create_table_if_not_exists(connection, 'combat', tquery_combat)

                except pymysql.MySQLError as e:
                    print(f"Error while creating tables: {e}")
                finally:
                    # Close the connection after performing the table creation
                    connection.close()
            else:
                print(f"Failed to reconnect to the database '{db_name}' after creation.")

    else:
        print("Connection to the MySQL server failed.")


if __name__ == "__main__":
    pass
    # BOTH WORK
    # query_delete_db(Server1, "DBTest1")
    # setup_db_and_tables(Server1)

    # This also works

    # sample_data = {
    #     'kills': 10,
    #     'accuracy': 85.5,
    #     'shots_fired': 200,
    #     'deaths': 2,
    #     'stims_used': 5,
    #     'accidentals': 1,
    #     'samples_extracted': 3,
    #     'stratagems_used': 4,
    #     'melee_kills': 2,
    #     'times_reinforcing': 3,
    #     'friendly_fire_damage': 50,
    #     'distance_travelled': 1000
    # }
    #
    # query_put_row(Server1, 'combat', **sample_data)
    #
    # print(query_get_data_from_table(Server1, 'combat'))

    # This works
    # sample_data = {
    #     'requisition': 100,
    #     'medals': 5,
    #     'xp': 2500
    # }
    # query_put_row(Server1, 'currency_gained', **sample_data)
    # print(query_get_data_from_table(Server1, 'currency_gained'))
