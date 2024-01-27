import os
import shutil

class DBFileCopier:

    def copy(self, source, destination):
        if os.path.exists(source):
            if os.path.exists(destination):
                shutil.rmtree(destination)
            shutil.copytree(source, destination)
            return True
        else:
            return False
        
def main(source, destination):
    dbfilecopier = DBFileCopier()
    dbfilecopier.copy(source=source, destination=destination)

if __name__ == '__main__':
    main(source=r'C:\_repos\crube_videos_database\test\sampledata_source', destination=r'C:\_repos\crube_videos_database\test\sampledata_destination')