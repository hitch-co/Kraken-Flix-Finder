import sqlite3

class DBExplorer:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = self._connect_to_db(self.db_path)
        self.cursor = self.conn.cursor()

    def _connect_to_db(self, db_path):
        conn = sqlite3.connect(db_path)
        return conn

    def load_sql_file_from_path(self, path):
        with open(path, 'r') as f:
            sql = f.read()

        print("LOG: This is the SQL imported:")
        print(sql)
        return sql    

    def query_sql(self, sql):
        self.cursor.execute(sql)
        rows = self.cursor.fetchall()
        
        print(f"Number of rows: {str(len(rows))}")
        return rows
                    
    def get_all_tables(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()

        print("LOG: These are the tables in the database:")
        for table in tables:
            print(table)
        return tables
    
    def get_table_schema(self, table_name):
        self.cursor.execute("PRAGMA table_info('"+table_name+"');")
        columns = self.cursor.fetchall()
        print("LOG: These are the columns in the table: "+table_name)
        for column in columns:
            print(column)
        return columns

    def get_table_rows(self, table_name):
        self.cursor.execute("SELECT * FROM "+table_name+";")
        rows = self.cursor.fetchall()
        print("LOG: These are the rows in the table: "+table_name)
        for row in rows:
            print(row)
        return rows

    def close_connection(self):
        self.conn.close()

    # Write list of tuples to csv
    def write_to_csv(self, results, file_path):
        import csv
        with open(file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(results)
            
def main(db_path, query_path):
    dbexplorer = DBExplorer(db_path=db_path)
    sql = dbexplorer.load_sql_file_from_path(path=query_path)
    results = dbexplorer.query_sql(sql=sql)
    for i, row in enumerate(results):
        if i < 5:
            print(f"Row {i}: {row}")

    #write to csv
    dbexplorer.write_to_csv(results=results, file_path='./csv/horror_movies.csv')

if __name__ == '__main__':
    db_path = './db/MyVideos116/MyVideos116.db'
    query_path = './sql/horror_movies_final.sql'
    main(db_path=db_path, query_path=query_path)
    