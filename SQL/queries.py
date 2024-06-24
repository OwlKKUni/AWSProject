import os
import pymysql


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
        print("Connection successful")
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
def query_create_tables(server_name: DBConnString, table_queries: list) -> None:
    connection = connect(server_name)
    if connection:
        try:
            with connection.cursor() as cursor:
                for table_query in table_queries:
                    cursor.execute(table_query)
                    print(f'Table "{table_query.split(" ")[2]}" created')
                connection.commit()
        except pymysql.MySQLError as e:
            print(f'Error in query_create_tables()')
            print(f"Error creating tables: {e}")
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
# test this if it works

def query_update_cell(server_name: DBConnString, table_name: str, column_name: str, id_: int, value: any) -> None:
    sql_query = f"UPDATE {table_name} SET {column_name} = ? WHERE id = ?"

    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query, (value, id_))
            connect(server_name).commit()
        print(f'Table "{table_name}" updated')

    except pymysql.MySQLError as e:
        print(f'Error in query_update_cell()')
        print(f"Error updating table '{table_name}': {e}")
    except Exception as e:
        print(f'Error in query_update_cell()')
        print(f"An unexpected error occurred: {e}")


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


def query_delete_row(server_name: DBConnString, table_name: str, id_value: int) -> None:
    sql_query = f"DELETE FROM {table_name} WHERE id = %s"

    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query, (id_value,))
            connect(server_name).commit()
        print(f"Row with ID {id_value} deleted from table '{table_name}'")

    except pymysql.MySQLError as e:
        print(f'Error in query_delete_row()')
        print(f"Error deleting row from table '{table_name}': {e}")
    except Exception as e:
        print(f'Error in query_delete_row()')
        print(f"An unexpected error occurred: {e}")


# PUT
# ----------------------------------------------
# WORKS
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
            return result['id'] if result else None  # Access the result using the column name

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
    with connect(server_name).cursor() as cursor:
        cursor.execute(f'SELECT * FROM {table} WHERE id = {id_value}')
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        data["columns"] = columns
        data["rows"] = [row for row in rows]
    return data


if __name__ == "__main__":
    # does not fucking work for some reason
    print(query_get_data_from_table(Server1, 'currency_gained'))
    query_delete_row(Server1, 'currency_gained', 1)
    print(query_get_data_from_table(Server1, 'currency_gained'))

    # Fix this once you fix put
    # print('\n')
    # query_read_row(Server1, 'currency_gained', 0)
    #
    # print('\n')
    # query_read_table(Server1, 'currency_gained')

    # This creates "IF" tables ???
    # query_create_tables(Server1, [tquery_objectives, tquery_combat, tquery_samples, tquery_currency])
