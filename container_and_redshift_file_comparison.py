from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import sys, getopt
import pandas as pd
from datetime import datetime, timedelta
from s3_uploader import *
import os
import shutil
import time
import glob
import chardet
import psycopg2
import subprocess
from azure.core.exceptions import ResourceNotFoundError
from azure_email_notification import *
sys.path.append(os.path.abspath('D:\\script_monitoring'))
from completion_script import *

missing_files = 'D:\\dstdw\\Azure_blob\\daily_files\\missed_files'
missed_s3_path = 's3://desototech/Azure_jazz/Daily_files/missed_files'
log_dir = f'D:\\dstdw\\Azure_blob\\log_files'
run_dt = datetime.now().strftime("%Y%m%d")

#Create log file
sys.stdout = open(f"{log_dir}\\containers_and_redshift_comparison_{run_dt}.log", 'w')
sys.stderr = open(f"{log_dir}\\containers_and_redshift_comparison_{run_dt}.log", 'a')

starttime = datetime.now().strftime('%F %T')

print(f'------------------- Sctipt Starttime : {starttime} --------------------------')

#Get the Azure credentials from the json file
def get_azure_credentials():
    # Write the JSON object to a file
    
    json_path = f'D:\\dstdw\\Azure_blob\\credentials.json'
    
    with open(json_path) as json_file:
        json_data = json.load(json_file)
           
    storage_account_name = json_data['storage_account_name']
    storage_account_key = json_data['storage_account_key']
    az_success_container_name = json_data['az_success_container_name']
    az_archive_container_name = json_data['az_archive_container_name']
    file_prefix = json_data['file_prefix']
    
    return storage_account_name,storage_account_key,az_success_container_name,az_archive_container_name,file_prefix
    
#Function will intialize the Azure client using accout name and key
def initialize_azure_client():
    
    storage_account_name = get_azure_credentials()[0]
    storage_account_key = get_azure_credentials()[1]
    
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key)
    
    return blob_service_client

#Function will return the Total count from the 'data-success-prod' container 
def get_success_container_blob_count():
    
    blob_service_client = initialize_azure_client()
    
    az_success_container_name = get_azure_credentials()[2]
    file_prefix = get_azure_credentials()[4]
    
    success_client = blob_service_client.get_container_client(az_success_container_name)
    
    success_container_blob = []
    
    for blob in success_client.list_blob_names(name_starts_with=file_prefix): #file_prefix
        success_container_blob.append(blob)

    return success_container_blob

#Function will return the Total count from the 'archive-data-success-prod' container   
def get_archive_container_blob_count():
    
    blob_service_client = initialize_azure_client()
    
    az_archive_container_name = get_azure_credentials()[3]
    file_prefix = get_azure_credentials()[4]
    
    archive_client = blob_service_client.get_container_client(az_archive_container_name)
    
    archive_blob = []
    
    for blob in archive_client.list_blob_names(name_starts_with=file_prefix): #file_prefix
        archive_blob.append(blob)

    return archive_blob

#Get the connection details of Redshift
def read_redshift_conf():
    
    # Write the JSON object to a file
    #json_path = f'{script_path}\credentials.json'
    json_path = f'D:\\dstdw\\Azure_blob\\redshift_cred_prod.json'
    
    with open(json_path,'r') as json_file:
        json_data = json.load(json_file)
        
    
    dbname = json_data['DBNAME']
    host = json_data['HOST']
    port = json_data['PORT']
    user = json_data['USER']
    password = json_data['PASSWORD']
    s3_path = json_data['S3PATH']
    
    return dbname,host,port,user,password,s3_path

#Get the distinct file_name from the Redshift table    
def get_redshit_distinct_file_count(redshift_file_count):
    #print('inside ex_sql_file()')
    
    #Get parameters for Redshift connection
    params = read_redshift_conf()
    
    s3_path = params[5]
    #s3_path =s3_path.replace('RUN_DATE',run_date)
    print(f"-----------------------------------------------------------------")
    print("s3 file path: ",s3_path)
    
    print('all well')
    
    Total_count = "select count(*) from (select distinct(file_name) from jesta_prod_ft.jazz_orders_info);"
    print('--------------------------------------------------------------------------------------------')
    print("\n Total count of table is :", Total_count)

    #conn = psycopg2.connect(dbname=params['DBNAME'], host=params['HOST'], port=params['PORT'], user=params['USER'], password=params['PASSWORD'])
    conn = psycopg2.connect(dbname=params[0], host=params[1], port=params[2], user=params[3], password=params[4])
    cur = conn.cursor();
    print('Connection has been established!!')
    
    cur.execute("begin;")
    cur.execute(Total_count)
    count_tup = cur.fetchall()[0]
    count = ''.join(map(str,count_tup))
    #msg = count
    print(f"-----------------------------------------------------------------")
    print(count)
    redshift_file_count.append(count)
    cur.execute("commit;")

    conn.close()
    #print("Copy commands for jesta_prod_ft.jazz_orders_info table executed fine!")
    print(f'*********** Redshift count query completed : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ************')
    print("**************************************************************************")

def create_folders():
    
    today = datetime.now().strftime("%Y%m%d")
    #timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    #two_days_before = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    #two_days_before = '2024-07-06'
    
    print('------------------------------------------------------------------------')
    print("Today's date is: ",today)
#     print("Current timestamp : ",timestamp)
    print('------------------------------------------------------------------------')
    #out_file_path = f'{ofile_path}\\{today}'

    run_date_folder = os.path.join(missing_files,today)
    if not os.path.exists(run_date_folder):
        os.mkdir(run_date_folder)
        print("Folder is created:",run_date_folder)
        #glob_folders.append(run_date_folder)
    else:
        print(f'Path is already exists: ',run_date_folder)
        #glob_folders.append(run_date_folder)

    #Make variable as global so it can be used any where in the script
    listOfGlobals = globals()
    listOfGlobals['run_date'] = today
    #listOfGlobals['current_timestamp'] = timestamp
    listOfGlobals['missing_files_folder'] = run_date_folder
        
    print('********************************************')
    print('Missing files path is:',missing_files_folder)
    print('********************************************')
   
def create_distinct_filename_csv():
    #print('inside ex_sql_file()')
    
    #Get parameters for Redshift connection
    params = read_redshift_conf()
    
    s3_path = params[5]
    #s3_path =s3_path.replace('RUN_DATE',run_date)
    print(f"-----------------------------------------------------------------")
    print("s3 file path: ",s3_path)
    
    print('all well')

    #Total_count = "select count(*) from (select distinct(file_name) from jesta_prod_ft.jazz_orders_info);"
    print('--------------------------------------------------------------------------------------------')
    #print("\n Total count of table is :", Total_count)

    #conn = psycopg2.connect(dbname=params['DBNAME'], host=params['HOST'], port=params['PORT'], user=params['USER'], password=params['PASSWORD'])
    conn = psycopg2.connect(dbname=params[0], host=params[1], port=params[2], user=params[3], password=params[4])
    cur = conn.cursor();
    print('Connection has been established!!')
    
    cur.execute("begin;")
    file_names = pd.read_sql('select distinct(file_name) from jesta_prod_ft.jazz_orders_info order by file_name',conn)
    file_names.to_csv(f'{missing_files_folder}\\redshift_distinct_files.csv',index=False,header=False)

    conn.close()
    #print("Copy commands for jesta_prod_ft.jazz_orders_info table executed fine!")
    print(f'*********** Redshift command completed : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ************')
    print("**************************************************************************")

#Compare count of both containers with count of Redshift file_name 
def compare_container_and_redshift_count():
    redshift_file_count = []
    
    success_container_count = get_success_container_blob_count()
    archive_container_count = get_archive_container_blob_count()
    
    redshift_distinct_files_count = get_redshit_distinct_file_count(redshift_file_count)
    
    print('_______________________________________________________________')
    print('Success container count :',len(success_container_count))
    print('Archive_container_count :',len(archive_container_count))
    print('Redshift Table count :',redshift_file_count[0])
    print('_______________________________________________________________')
    
    
    total_blobs = len(success_container_count) + len(archive_container_count)
    print('Total blobs in both the containers :',total_blobs)
    
    if int(redshift_file_count[0]) == total_blobs:
        print('Redshift files count and both containers files are matched')
        create_script_status_file('container_and_redshift_file_comparison','daily','Jezz','CBIindia',starttime,datetime.now().strftime('%Y%m%d%H%M%S'))
        exit()
    else:
        print('There are some files missed to process. Please check!')
        create_folders() #Create a folder
        print('--------------------------------------------------------------')
        print("Create DataFrame of list of files from both the containers")
        
        archive_df = pd.DataFrame(archive_container_count)
        success_df = pd.DataFrame(success_container_count)
        
        #archive_df.to_csv(f'D:\\Users\\Siddharth\\Desoto\\my_task\\sprint_114\\AZURE_Blob\\daily_files\\missed_files\\archive_files.csv',index=False,header=False)
        #success_df.to_csv(f'D:\\Users\\Siddharth\\Desoto\\my_task\\sprint_114\\AZURE_Blob\\daily_files\\missed_files\\success_files.csv',index=False,header=False)
        
        #Combined both the containers file list
        archive_success_files_df = pd.concat([archive_df,success_df])
        archive_success_files_df.to_csv(f'{missing_files_folder}\\containers_files.csv',index=False,header=False)
        
        #Function will generate list of distinct file_name csv from the redshift table
        print(f'*********** Redshift Get Distinct file_name started : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ************')
        create_distinct_filename_csv()
        print('List of distinct file_name from the Redshift table has been created')
        print(f'*********** Redshift Get Distinct file_name Completed : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ************')
        
        print("Let's find out missing files")
        
        #Read achive and success containers files
        both_containers_df = pd.read_csv(f'{missing_files_folder}\\containers_files.csv',header=None)
        
        #Read Distinct file_name from redshift table
        redshift_file_df = pd.read_csv(f'{missing_files_folder}\\redshift_distinct_files.csv',header=None)
        
        #Convert the dataframes to sets of filenames
        both_containers_set = set(both_containers_df[0])
        redshift_file_set = set(redshift_file_df[0])
        
        missing_files_list = both_containers_set - redshift_file_set
        
        total_missed_files = len(list(missing_files_list))
        
        missed_file_df = pd.DataFrame(list(missing_files_list))
        missed_file_df.to_csv(f'{missing_files_folder}\\missed_files_list.csv',index=False,header=None)
        print('---------------------------------------------------------------------')
        print(f'Missed file has been created : {missing_files_folder}')
        
        #Upload modified files(csv) on s3
        s3_path = f'{missed_s3_path}/{run_date}/missed_files_list.csv'
        local_file_path = f'{missing_files_folder}\\missed_files_list.csv'
        
        with open(os.path.join(log_dir, f"s3upload_daily_{run_date}.log"), "a") as log_file:
            subprocess.run(["aws", "s3", "cp", local_file_path, f"{s3_path}"],stdout=log_file, stderr=subprocess.STDOUT, shell=True)
        
        create_script_status_file('container_and_redshift_file_comparison','daily','Jezz','CBIindia',starttime,datetime.now().strftime('%Y%m%d%H%M%S'))
        missed_files_notification(starttime,run_date,total_missed_files,s3_path)
        
        print(f'Missed file uploaded on s3 : {s3_path}')
        print('Script successfully completed')
        print('_______________________________________________________')
        print(f'Total missed file count is : {total_missed_files}')
        print('_______________________________________________________')
        #missed_files_notification(starttime,run_date,total_missed_files,s3_path)
        print('Email has been sent!!')
        print('___________________________________________________________________________________')
        
if __name__ == "__main__":
    print("calling function to compair both containers and Redshift files count")
    compare_container_and_redshift_count()