import os
import shutil

#NOTES: 
# - Need to be careful of source/destination drives.
# - Don't currently have a drive to test with

class DBFileCopier:

    def copy(self, source, destination_drive):
        source_path_except_drive = source.split(':')[1]
        destination_path = os.path.join((destination_drive+":"), source_path_except_drive)
        if os.path.exists(source):
            if os.path.exists(destination_path):
                shutil.rmtree(destination_path)
            shutil.copytree(source, destination_path)
            return True
        else:
            return False
        
def main(source, destination):
    dbfilecopier = DBFileCopier()
    dbfilecopier.copy(source=source, destination=destination)

if __name__ == '__main__':
    main(source=r'C:\_repos\crube_videos_database\test\sampledata_source', destination=r'C:\_repos\crube_videos_database\test\sampledata_destination')