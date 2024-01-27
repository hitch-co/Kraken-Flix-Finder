from classes.DBExplorer import DBExplorer
from classes.DBFileCopier import DBFileCopier

db_path = './db/MyVideos116/MyVideos116.db'
query_path = './sql/horror_movies_final.sql'

# DB Explorer
dbexplorer = DBExplorer(db_path=db_path)
sql = dbexplorer.load_sql_file_from_path(path=query_path)
results = dbexplorer.query_sql(sql=sql)
for i, row in enumerate(results):
    if i < 5:
        print(f"Row {i}: {row}")