import os, time, shutil

#source_folder = r"\\10.240.160.176\f$"
today = time.time()
source_location = r"\\10.240.160.176\f$\Network Exporter\Archive"
destination_location = r"\\10.240.160.176\g$\NetworkExporterArchive"
for folder in os.listdir(source_location):
    folder_creation_time = os.stat(source_location+'/'+ folder).st_ctime
    if today - folder_creation_time > 604800:
        print(folder,os.stat(source_location+'/'+ folder).st_ctime)
        shutil.move(source_location+'/'+ folder,destination_location+'/'+ folder)
        shutil.make_archive()
        #, dt.datetime.fromtimestamp(os.stat(source_location+'/'+ folder).st_ctime).strftime("%A, %B %d"))#os.stat(root+'/'+folder).st_ctime)
        # if FeederList in file:
        #     FileChoosen[1] = max(FileChoosen[1], os.path.getctime(root+'/'+file))