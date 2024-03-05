import os
import shutil

#NOTES: 
# - Need to be careful of source/destination drives.
# - Don't currently have a drive to test with

class DBFileCopier:

    def copy(self, source, destination_drive, destination_folder=None):
        source_path_except_drive = None

        # If destination_folder is not None, then replace it directly with the destination_path
        if destination_folder is not None:
            destination_path = destination_folder
        else:
            source_path_except_drive = source.split(':')[1]
            destination_path = os.path.join((destination_drive+":"), source_path_except_drive)
            
        if source_path_except_drive:
            print(f"source path except drive: {source_path_except_drive}")
        print(f"source: {source}")
        print(f"destination: {destination_path}")

        if os.path.exists(source):
            # if os.path.exists(destination_path):
            #     #shutil.rmtree(destination_path)
            shutil.copytree(source, destination_path, dirs_exist_ok=True)
            return True
        else:
            return False
        
def main(source, destination_drive, destination_folder = None):
    dbfilecopier = DBFileCopier()
    dbfilecopier.copy(
        source=source, 
        destination_drive=destination_drive, 
        destination_folder=destination_folder
        )

if __name__ == '__main__':
    main(
        source=r'C:\_repos\Kraken-Flix-Finder\test\sampledata_source', 
        destination_drive=r'C', 
        destination_folder=r'C:\_repos\Kraken-Flix-Finder\test\sampledata_destination'
        )