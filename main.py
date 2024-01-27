from classes.DBExplorer import DBExplorer
from classes.DBFileCopier import DBFileCopier

#NOTES
# - Need to be careful of source/destination drives.  
# - Don't currently have a drive to test with

db_path = './db/MyVideos116/MyVideos116.db'
query_path = './sql/horror_movies_final.sql'

# DB EXplorer
dbexplorer = DBExplorer(db_path=db_path)
sql = dbexplorer.load_sql_file_from_path(path=query_path)
results = dbexplorer.query_sql(sql=sql)
for i, row in enumerate(results):
    if i < 5:
        print(f"Row {i}: {row}")

# DB File Copier
dbfilecopier = DBFileCopier(destination_folder)

for path, title in items:
    source = r'C:\_repos\crube_videos_database\test\sampledata_source'
    destination = r'C:\_repos\crube_videos_database\test\sampledata_destination'
    
    dbfilecopier.copy(source=source, destination=destination)

    dbfilecopier.copy()