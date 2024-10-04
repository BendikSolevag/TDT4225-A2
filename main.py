from DbConnector import DbConnector
from tabulate import tabulate


class ExampleProgram:

    def insert_data(self, table_name):
        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        query = """
                CREATE TABLE IF NOT EXISTS User (
                   id VARCHAR(3) NOT NULL,
                   has_labels BOOLEAN DEFAULT false,
                   PRIMARY KEY (id)
                )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        query = """
                CREATE TABLE IF NOT EXISTS Activity (
                    id INT NOT NULL,
                    user_id VARCHAR(3) NOT NULL,
                    transportation_mode VARCHAR(15),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    PRIMARY KEY (id),
                    FOREIGN KEY (user_id) REFERENCES User(id) ON DELETE CASCADE
                )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        query = """
                CREATE TABLE IF NOT EXISTS TrackPoint (
                    id INT NOT NULL,
                    activity_id INT NOT NULL,
                    lat DOUBLE,
                    lon DOUBLE,
                    altitude INT,
                    date_time DATETIME,
                    PRIMARY KEY (id),
                    FOREIGN KEY (activity_id) REFERENCES Activity(id) ON DELETE CASCADE
                )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()
    
    def insert_users(self):
        pass





def main():
    program = None
    try:
        program = ExampleProgram()
        program.create_tables()
        
        # Check that the table is dropped
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
