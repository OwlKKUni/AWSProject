import os
import pyodbc


class DBConnString:
    def __init__(self, server, database, username, password, driver):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver


class SQLQuery:
    def __init__(self, table_name, **kwargs):
        self.table_name = table_name
        self.columns = kwargs

    def add_column(self, column_name, column_type):
        self.columns[column_name] = column_type

    def generate_query(self):
        query = f"CREATE TABLE {self.table_name} ("
        column_definitions = [f"id INT PRIMARY KEY"]  # first column
        for column_name, column_type in self.columns.items():
            column_definitions.append(f"{column_name} {column_type}")
        query += ", ".join(column_definitions)
        query += ");"
        return query


# CONN STRING FOR SERVERS
Server1 = DBConnString(os.environ['AZURE_SERVER'],
                       os.environ['AZURE_DATABASE'],
                       os.environ['AZURE_SERVER_USERNAME'],
                       os.environ['AZURE_DB_PASSWORD'],
                       '{ODBC Driver 18 for SQL Server}')

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


def connect(conn_string):
    try:
        conn = pyodbc.connect(f'DRIVER={conn_string.driver};SERVER={conn_string.server};'
                              f'DATABASE={conn_string.database};UID={conn_string.username};'
                              f'PWD={conn_string.password}')
        return conn
    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")
        return None


# CREATE TABLES
# ----------------------------
def query_create_tables(server_name: DBConnString, table_queries: list) -> None:
    try:
        if connect(server_name) is None:
            print("Failed to connect to the database.")
            return

        with connect(server_name).cursor() as cursor:
            for table_query in table_queries:
                cursor.execute(table_query)
                print(f'Table "{table_query.split(" ")[2]}" created')
            connect(server_name).commit()

    except pyodbc.Error as e:
        print(f"Error creating data: {e}")


# READ TABLES
# ----------------------------
def query_read_row(server_name: DBConnString, table: str, row_number: int) -> None:
    with connect(server_name).cursor as cursor:
        row = cursor.execute(f'SELECT * FROM {table} WHERE rowid = ?', (row_number,))
        print(row)


def query_read_table(server_name: DBConnString, table: str) -> None:
    with connect(server_name).cursor as cursor:
        cursor.execute(f'SELECT * FROM {table}')
        rows = cursor.fetchall()
        for row in rows:
            print(row)


# GET TABLES
# ----------------------------
# Works
def query_get_table_names(server_name: DBConnString):
    sql_query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'"

    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query)
            table_names = [row.TABLE_NAME for row in cursor.fetchall()]

        if table_names:
            return table_names

        else:
            print("No data found.")
            return []

    except pyodbc.Error as e:
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


#
def query_get_data_from_table(server_name: DBConnString, table: str) -> list:
    with connect(server_name).cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table}")
        columns = [column[0] for column in cursor.description]
        rows = cursor.fetchall()
        data = [columns] + [list(row) for row in rows]
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

    except pyodbc.Error as e:
        print(f"Error updating table '{table_name}': {e}")
    except Exception as e:
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
        print(f"An unexpected error occurred: {e}")


# WORKS
def query_delete_table(server_name: DBConnString, table_name: str) -> None:
    sql_query = f"IF OBJECT_ID('{table_name}', 'U') IS NOT NULL DROP TABLE {table_name}"

    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query)
            connect(server_name).commit()
        print(f'Table "{table_name}" deleted')

    except pyodbc.Error as e:
        print(f"Error deleting table '{table_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# def query_delete_row(server_name: DBConnString, table_name: str, row_number: int) -> None:
#     sql_query = f"DELETE FROM {table_name} WHERE rowid = {row_number}"
#
#     with connect(server_name).cursor() as cursor:
#         cursor.execute(sql_query)
#         connect(server_name).commit()

def query_delete_row(server_name: DBConnString, table_name: str, id_value: int) -> None:
    sql_query = f"DELETE FROM {table_name} WHERE id = ?"

    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query, (id_value,))
            connect(server_name).commit()
        print(f"Row with ID {id_value} deleted from table '{table_name}'")

    except pyodbc.Error as e:
        print(f"Error deleting row from table '{table_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# PUT
# ----------------------------------------------
def query_put_row(server_name: DBConnString, table_name: str, **kwargs) -> None:
    # Extract columns and values from kwargs
    columns = ', '.join(kwargs.keys())
    values_placeholders = ', '.join(['?'] * len(kwargs))
    values = tuple(kwargs.values())

    sql_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values_placeholders})"

    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query, values)
            connect(server_name).commit()
        print(f"Row inserted into table '{table_name}'")

    except pyodbc.Error as e:
        print(f"Error inserting row into table '{table_name}': {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


# Aux functions
# ------------------------
def query_get_last_id_value(server_name: DBConnString, table_name: str) -> int:
    sql_query = f"SELECT TOP 1 id FROM {table_name} ORDER BY id DESC"
    try:
        with connect(server_name).cursor() as cursor:
            cursor.execute(sql_query)
            result = cursor.fetchone()
            return result[0] if result else None

    except pyodbc.Error as e:
        print(f"Error occurred: {e}")
        return None


# dict {columns: [], rows: [()]}
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
    print(query_get_data_by_id(Server1, 'objectives_completed', 1))
