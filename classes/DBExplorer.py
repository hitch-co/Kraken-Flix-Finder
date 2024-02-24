import sqlite3

from classes.LoggingManager import LoggingManager

class DBExplorer:
    def __init__(self, db_path):
        self.db_path = db_path

        self.logger = LoggingManager(
            dirname='log', 
            logger_name='DBExplorer', 
            debug_level='DEBUG', 
            mode='w', 
            stream_logs=True, 
            encoding='UTF-8'
            ).create_logger()

    def load_sql_file_from_path(self, path):
        with open(path, 'r') as f:
            sql = f.read()

        self.logger.debug("LOG: This is the SQL imported:")
        self.logger.debug(sql)
        return sql    

    def query_sql(self, sql, params=None):
        with sqlite3.connect(database = self.db_path) as conn:
            cursor = conn.cursor()
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            rows = cursor.fetchall()
     
        self.logger.info(f"Number of rows: {str(len(rows))}")

        #Translate the rows into a list of dictionaries
        if len(rows) > 0:
            keys = [description[0] for description in cursor.description]
            rows = [dict(zip(keys, row)) for row in rows]
            self.logger.debug(f"LOG: Top 5 rows: {rows[:5]}")
            return rows
        else:
            self.logger.debug("LOG: No rows returned")
            return []
                    
    def get_all_tables(self):
        self.cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = self.cursor.fetchall()

        self.logger.info("LOG: These are the tables in the database:")
        for table in tables:
            self.logger.debug(table)
        return tables
    
    def get_table_schema(self, table_name):
        self.cursor.execute("PRAGMA table_info('"+table_name+"');")
        columns = self.cursor.fetchall()
        self.logger.info(f"LOG: These are the columns in the table: {table_name}")
        for column in columns:
            self.logger.info(column)
        return columns

    def get_table_rows(self, table_name):
        self.cursor.execute("SELECT * FROM "+table_name+";")
        rows = self.cursor.fetchall()
        self.logger.debug("LOG: These are the rows in the table: {table_name}")
        for row in rows:
            self.logger.debug(row)
        return rows

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
    query_path = './sql/front-end-search-query_parameterless.sql'
    main(db_path=db_path, query_path=query_path)
    