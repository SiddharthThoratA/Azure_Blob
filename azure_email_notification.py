import os
import time
import glob
import subprocess
from datetime import datetime, timedelta


config_file_path = f'D:\\dstdw\\Azure_blob\\email_config.config'
run_date = datetime.now().strftime("%Y%m%d")

#log_file_email = f"email_notication_20240806.log"

#Start email notification
def start_email_notification(start_time,timestamp):
    
    email_start_subject = f"Azure blob data extraction and load Job started - azure_download_files.py for {timestamp}"
    #print(email_start_subject)
    
    email_start_body = f"""
Job azure_download_files.py for Azure data extraction and load (freq - 2 hrs) has started for the day - {timestamp}

Start time : {start_time}
    """
    
    config_file = f'{config_file_path}'
    
    log_file_email = f'D:\\dstdw\\Azure_blob\\log_files\\email_notification_{run_date}.log'
    
    # call python start email send script
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
    
    print('_________________________________________________________________________________________')

#Send completion email with files count   
def completion_email_notification(start_time,timestamp,file_counts,table_count):
    
    email_start_subject = f"Azure blob data extraction and load Job completed - azure_download_files.py for {timestamp}"
    #print(email_start_subject)
    
    email_start_body = f"""
Job azure_download_files.py for Azure data extraction and load (freq - 2 hrs) has completed for the day - {timestamp}

Total Files Loaded : {file_counts}
Table Name : jesta_prod_ft.jazz_orders_info
Table count : {table_count} 

Start time : {start_time}
completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    
    config_file = f'{config_file_path}'
    
    log_file_email = f'D:\\dstdw\\Azure_blob\\log_files\\email_notification_{run_date}.log'
    
    # call python start email send script
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
    
    print('_________________________________________________________________________________________')

#Send email when there is no any latest files to process
def no_latest_blob_available(start_time,timestamp,file_counts):

    email_start_subject = f"Azure blob data extraction and load Job - No new file found for {timestamp}"
    #print(email_start_subject)
    
    email_start_body = f"""

No new file found in data-success-prod container for - {timestamp}

Total files loaded : {file_counts}

Start time : {start_time}
completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    
    config_file = f'{config_file_path}'
    
    log_file_email = f'D:\\dstdw\\Azure_blob\\log_files\\email_notification_{run_date}.log'
    
    # call python start email send script
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)


    print('_________________________________________________________________________________________')
    
#Send email if success prod container is empty
def empty_container(start_time,timestamp):

    email_start_subject = f"Alert! Azure Data-Success-Prod container is empty for {timestamp}"
    #print(email_start_subject)
    
    email_start_body = f"""

Container is empty. Please look into it.

Container Name : data-success-prod

Start time : {start_time}
completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    
    config_file = f'{config_file_path}'
    
    log_file_email = f'D:\\dstdw\\Azure_blob\\log_files\\email_notification_{run_date}.log'
    
    # call python start email send script
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
        
    print('_________________________________________________________________________________________')   

#Send email if any file is failed to convert into csv
def failed_files_notification(start_time,timestamp,s3_path):

    email_start_subject = f"Alert! Failed to convert Azure JSON files to CSV for {timestamp}"
    #print(email_start_subject)
    
    email_start_body = f"""

Some of the files are not converted into CSV, please find list of files on below path:

Failed_Files_S3_Path : {s3_path}

Start time : {start_time}
completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    
    config_file = f'{config_file_path}'
    
    log_file_email = f'D:\\dstdw\\Azure_blob\\log_files\\email_notification_{run_date}.log'
    
    # call python start email send script
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
    
    print('_________________________________________________________________________________________')

def missed_files_notification(start_time,timestamp,missed_file_counts,missed_s3_path):

    email_start_subject = f"Alert!Missed file found for Azure blob data extraction and load Job for {timestamp}"
    #print(email_start_subject)
    
    email_start_body = f"""

Please find missed files list on below s3 path: 

Total missed files count : {missed_file_counts}
s3_path = {missed_s3_path}

Start time : {start_time}
completion time : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    """
    
    config_file = f'{config_file_path}'
    
    log_file_email = f'D:\\dstdw\\Azure_blob\\log_files\\email_notification_{run_date}.log'
    
    # call python start email send script
    with open(log_file_email, "a") as f:
        subprocess.run(["python",f"email_notification.py", "--s",email_start_subject ,"--b",email_start_body, "--c", config_file], stdout=f)
    
    print('_________________________________________________________________________________________')