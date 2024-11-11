#importing required libaries
import sys, getopt
#import datetime
import csv
import psycopg2
import subprocess
import shutil
import requests
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import json
import pandas as pd
from datetime import datetime, timedelta
from s3_uploader import *
import os
import time
import glob
import chardet
from azure.core.exceptions import ResourceNotFoundError
from azure_email_notification import *
sys.path.append(os.path.abspath('D:\\script_monitoring'))
from completion_script import *

script_path = f'D:\\dstdw\\Azure_blob\\'
json_f_path = f'D:\\dstdw\\Azure_blob\\daily_files\\json_files'
mod_f_path = f'D:\\dstdw\\Azure_blob\\daily_files\\modified_files'
downloaded_files_list = f'D:\\dstdw\\Azure_blob\\daily_files\\downloaded_files_list'
failed_files_list = f'D:\\dstdw\\Azure_blob\\daily_files\\failed_files_list'
prev_processed_f_path = f'D:\\dstdw\\Azure_blob\\daily_files\\success_prod_files_list\\prev_processed_files_list'
success_prod_f_path = f'D:\\dstdw\\Azure_blob\\daily_files\\success_prod_files_list'
log_dir = f'D:\\dstdw\\Azure_blob\\log_files'
bucket = 'desototech'
#s3_path = 'DWH_team/sid/Azure_blob_files'
#s3_path_mod_comb = 's3://desototech/Azure_jazz/History_files'
s3_path_mod_comb = 's3://desototech/Azure_jazz/Daily_files'
s3_path = 'Azure_jazz/Daily_files'
#s3_path = 'Azure_jazz/History_files'
run_dt = datetime.now().strftime("%Y%m%d%H%M%S")
starttime = datetime.now().strftime('%F %T')
os.chdir(script_path)

#Create log file
sys.stdout = open(f"{log_dir}\\azure_blob_archive_files_extraction_script_{run_dt}.log", 'w')
sys.stderr = open(f"{log_dir}\\azure_blob_archive_files_extraction_script_{run_dt}.log", 'a')

def create_folder(run_day,curr_timestamp):
    #Get current date
    today = run_day
    timestamp = curr_timestamp
    
    #today = datetime.now().strftime("%Y%m%d")
    #timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    #two_days_before = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
    #two_days_before = '2024-07-06'
    
#     print('------------------------------------------------------------------------')
#     print("Today's date is: ",today)
#     print("Current timestamp : ",timestamp)
#     print('------------------------------------------------------------------------')
    #out_file_path = f'{ofile_path}\\{today}'
    glob_folders = []
    
    folders_list = [json_f_path,mod_f_path,downloaded_files_list,failed_files_list,prev_processed_f_path]
    
    for folder in folders_list:
        run_date_folder = os.path.join(folder,today)
        if not os.path.exists(run_date_folder):
            os.mkdir(run_date_folder)
            print("Folder is created:",run_date_folder)
            #glob_folders.append(run_date_folder)
            
        else:
            print(f'Path is already exists: ',run_date_folder)
            #glob_folders.append(run_date_folder)
            
        timestamp_foder_path = os.path.join(run_date_folder,timestamp)
        if not os.path.exists(timestamp_foder_path):
            os.mkdir(timestamp_foder_path)
            print("TimeStamp Folder Created :",timestamp_foder_path)
        else:
            print(f"TimeStamp Folder is already exists: {timestamp_foder_path}")
        
        glob_folders.append(timestamp_foder_path)
    
    #Make variable as global so it can be used any where in the script
    listOfGlobals = globals()
    listOfGlobals['run_date'] = today
    listOfGlobals['current_timestamp'] = timestamp
    listOfGlobals['json_file_path'] = glob_folders[0]
    listOfGlobals['mod_file_path'] = glob_folders[1]
    listOfGlobals['downloaded_files_list_path'] = glob_folders[2]
    listOfGlobals['failed_files_list_path'] = glob_folders[3]
    listOfGlobals['prev_processed_files_list'] = glob_folders[4]
    
    print('********************************************')
    print('Json file path is:',json_file_path)
    print('-----------------')
    print('Modified file path is:',mod_file_path)
    print('-----------------')
    print('Downloaded files list path is:',downloaded_files_list_path)
    print('-----------------')
    print('Failed files list path is:',failed_files_list_path)
    print('-----------------')
    print('Previous Processed files list path is:',prev_processed_files_list)
    print('********************************************')

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

def initialize_azure_client():
    
    storage_account_name = get_azure_credentials()[0]
    storage_account_key = get_azure_credentials()[1]
    
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=storage_account_key)
    
    return blob_service_client

#Get the latest list of blob to download from the data-success-prod container
def get_list_of_latest_blobs():
    
    #Get current date and current timestamp
    today = datetime.now().strftime("%Y%m%d")
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    
    print('---------------------------------------------------------------------------------')
    print(f'------------------------ Script StartTime : {starttime} ------------------------')
    print("Today's date is: ",today)
    print("Current timestamp : ",timestamp)
    print('---------------------------------------------------------------------------------')
    
    #send start email notification
    start_email_notification(starttime,datetime.now().strftime("%Y%m%d%H%M%S"))
    #Send start email notification
    print('Start email sent')
    
    blob_service_client = initialize_azure_client()
    
    az_success_container_name = get_azure_credentials()[2]
    file_prefix = get_azure_credentials()[4]
    
    archive_client = blob_service_client.get_container_client(az_success_container_name)
    
    selected_blob = []
    
    for blob in archive_client.list_blob_names(name_starts_with=file_prefix): #file_prefix
        selected_blob.append(blob)
    
    print('____________________________________________________')
    print(f'Total bolbs in {az_success_container_name} : {len(selected_blob)}')
    print('____________________________________________________')
    
    if selected_blob:
        
        #Create DataFrame for current list of files from data_success_prod container
        curr_blob_df=pd.DataFrame(selected_blob)
        
        #Read previous file
        prev_blob_df = pd.read_csv(f"{success_prod_f_path}\\Current_blob_files.csv",header=None)
        
        # Convert the dataframes to sets of filenames
        prev_blob_set = set(prev_blob_df[0])
        curr_blob_set = set(curr_blob_df[0])
        
        #Minus previous blob list from current blob list and get the list of latest blob and download it
        diff_files = curr_blob_set - prev_blob_set
        
        #Last blob list
        latest_blobs_list = sorted(list(diff_files))
        
        print('______________________________________________________________________')
        print(f'Latest bolbs in {az_success_container_name} : {len(latest_blobs_list)}')
        print('______________________________________________________________________')
        
        
        if latest_blobs_list:
            
            #Call create folder function to create current timestamp folders in appropriet path
            create_folder(today,timestamp)
            
            print(f"Total Latest blob to be downloaded : {len(latest_blobs_list)} ")

            #Call function to download latest blob files from the container
            print('***************************************************')
            print("Calling download_data_success_prod function")
            print('***************************************************')
            download_data_success_prod(latest_blobs_list)
            
            
            #Copy previous loaded file list to prev_processed_files_list dated and timeframe folder 
            shutil.copy2(f"{success_prod_f_path}\\Current_blob_files.csv",f"{prev_processed_files_list}\\")
            print(f'Previous list of blobs file has been copied to : {prev_processed_files_list}')
            
            #Create csv file of current blob
            curr_blob_df.to_csv(f"{success_prod_f_path}\\Current_blob_files.csv",index=False,header=False)
        else:
            no_latest_blob_available(starttime,timestamp,len(latest_blobs_list))
            print(f"No latest blobs are avalilable to process in {az_success_container_name} at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"Total Latest blob to be downloaded : {len(latest_blobs_list)} ")
            print('___________________________________________________________________________________________')
            create_script_status_file('azure_download_files','2hour','Jezz','CBIindia',starttime,datetime.now().strftime('%Y%m%d%H%M%S'))
    else:
        empty_container(starttime,timestamp)
        print(f"No files are available in {az_success_container_name} at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print('___________________________________________________________________________________________')
        
    
#def download_archive_files():
def download_data_success_prod(selected_blob):
    #Create run_date folder 
    #create_folder()
    
    blob_service_client = initialize_azure_client()
    
    az_archive_container_name = get_azure_credentials()[3]
    az_success_container_name = get_azure_credentials()[2]
    file_prefix = get_azure_credentials()[4]
    
    #archive_client = blob_service_client.get_container_client(az_archive_container_name)
    archive_client = blob_service_client.get_container_client(az_success_container_name)
    
    #selected_blob = []
    modified_files = []
    failed_files = []
        
        
    #for blob in archive_client.list_blob_names(name_starts_with=file_prefix): #file_prefix
    #    selected_blob.append(blob)

    if selected_blob:
        #Create run_date folder 
        #create_folder()
        
        #print('___________________________________________________________________________________________')
        #print(f"List of files to be downloaded :",selected_blob)
        #print('___________________________________________________________________________________________')
        
        #Function will create list of files to be downloaded at currenttimestamp folder
        create_downloaded_files_list(selected_blob)
        
        print('--------------------------------------------------------------------------------------')

        for blob_name in selected_blob:
            #print(blob_name)
            blob_client = archive_client.get_blob_client(blob_name)
            download_file_path = f'{json_file_path}\\{blob_name}'

            #print(f'Downloding {blob_name} to {download_file_path}')
            try:  
                with open(download_file_path,'wb') as download_file:
                    download_file.write(blob_client.download_blob().readall())
                
                #print('----')
                #print(f'Calling Modified file function : File_Name : {blob_name} , File_path : {json_file_path}\\{blob_name}')
                #print('----')
                
                #Calling function to convert JSON data into CSV formate
                json_flatten_function(download_file_path,blob_name,failed_files,modified_files)
            
            except ResourceNotFoundError:
                print(f"File not found: {blob_name}")
                failed_files.append(blob_name)

            except Exception as e:
                print(f"Failed to download {blob_name}: {e}")
                failed_files.append(blob_name)

        #Call combined csv file function
        combined_mod_csv_files()
        print('-----------------------------------------------------------------------------------')
        print("All the csv file has been combined")
        print(f"File Path :{mod_file_path}\\combined_mod_azure_file_{current_timestamp}.csv")
        print('-----------------------------------------------------------------------------------')
        
        ##Upload modified files(csv) on s3
        #mod_s3_path = f'{s3_path}/modified_files/{run_date}/{current_timestamp}/'
        #multi_files_upload_s3(mod_file_path,bucket,mod_s3_path)
        
        #Upload modified files(csv) on s3
        mod_s3_path = f'{s3_path_mod_comb}/modified_files/{run_date}/{current_timestamp}/'
        combined_file_path = f'{mod_file_path}\\combined_mod_azure_file_{current_timestamp}.csv'
        
        with open(os.path.join(log_dir, f"s3upload_daily_{current_timestamp}.log"), "a") as log_file:
            subprocess.run(["aws", "s3", "cp", combined_file_path, f"{mod_s3_path}"],stdout=log_file, stderr=subprocess.STDOUT, shell=True)

        
        print(f'All Modified(CSV) files uploaded on s3: Current_TimeStamp: {current_timestamp}, s3_path:{mod_s3_path}')
        print('*****************************************************************************************************')
        
#        #Upload json files on s3
#        json_s3_path = f'{s3_path}/json_files/{run_date}/{current_timestamp}/'
#        multi_files_upload_s3(json_file_path,bucket,json_s3_path)
#        print('*****************************************************************************************************')
#        print(f'All JSON files uploaded on s3: Current_TimeStamp: {current_timestamp}, s3_path:{json_s3_path}')
#        print('*****************************************************************************************************')

        # Log failed files
        if failed_files:
            print('The following files failed to convert to CSV:')
            for failed_file in failed_files:
                print('XXXXXXXXXXXXXXXXXXXXXXXXXXX')
                print(failed_file)
                print('XXXXXXXXXXXXXXXXXXXXXXXXXXX')
                
            #Calling function to create list of failed files
            failed_to_create_csv(failed_files)
        
            #Upload Failed files list on s3
            failed_s3_path = f'{s3_path}/failed_files_list/{run_date}/{current_timestamp}/'
            multi_files_upload_s3(failed_files_list_path,bucket,failed_s3_path)
            print('___________________________________________________________________________________________')
            print('Failed files list uploaded to s3 completed')
            print('___________________________________________________________________________________________')
        
        #Upload Downloaded files list on s3
        downloaded_s3_path = f'{s3_path}/downloaded_files_list/{run_date}/{current_timestamp}/'
        multi_files_upload_s3(downloaded_files_list_path,bucket,downloaded_s3_path)
        print('___________________________________________________________________________________________')
        print('Downloaded files list uploaded to s3 completed')
        print('___________________________________________________________________________________________')
        

        #Copy file from source container to archive container
        #copy_source_to_archive(selected_blob)
        
        print(f'*********** Redshift load started : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ************')
        #Calling Redshift load function to load csv files from s3
        redshift_table_count = []
        redshift_load(redshift_table_count)  #Need to enable back
        
        print(f'Total Downloaded {len(selected_blob)} files')
        print(f'Total Modified {len(modified_files)} files')
        failed_to_modified(selected_blob,modified_files)
        
        print("----------- Seding Completion Email ------------")
        #completion_email_notification(starttime,datetime.now().strftime('%Y-%m-%d %H:%M:%S'),len(modified_files),redshift_table_count[0])
        completion_email_notification(starttime,current_timestamp,len(modified_files),redshift_table_count[0])
        create_script_status_file('azure_download_files','2hour','Jezz','CBIindia',starttime,datetime.now().strftime('%Y%m%d%H%M%S'))
    else:
        print('*****************************************************************************************************')
        print(f"No files are available in {az_success_container_name} at : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print('*****************************************************************************************************')
    
    #modified_files_count = glob.glob(f'{mod_file_path}\*')
    
        print(f'Total Downloaded {len(selected_blob)} files')
        print(f'Total Modified {len(modified_files)} files')
    
    
def create_downloaded_files_list(files_list):
    
    downloaded_files = pd.DataFrame(files_list)
    #final_path = f'{downloaded_files_list}\\{run_date}\\{current_timestamp}'
    #print('final_path is ',final_path)
    downloaded_files.to_csv(f'{downloaded_files_list_path}\\jazz_order_files_{current_timestamp}.csv',index=False,header=False)
    print(f"Latest files list has been created : {downloaded_files_list_path}\\jazz_order_files_{current_timestamp}.csv'")

def failed_to_create_csv(failed_list):
    failed_files = pd.DataFrame(failed_list)
    #final_path = f'{downloaded_files_list}\\{run_date}\\{current_timestamp}'
    #print('final_path is ',final_path)
    failed_files.to_csv(f'{failed_files_list_path}\\failed_jazz_order_files_{current_timestamp}.csv',index=False,header=False)

def failed_to_modified(json_file_list,mod_files_list):
    
    json_files_count = len(json_file_list)
    modified_files_count = len(mod_files_list)
    
    if json_files_count == modified_files_count:
        print('*************************************************************************************')
        print("All files have been modified successfully and uploaded on s3")
        print('*************************************************************************************')
    else:
        failed_files_s3_path = f'{s3_path}/failed_files_list/{run_date}/{current_timestamp}/'
        failed_files_notification(starttime,datetime.now().strftime('%Y-%m-%d %H:%M:%S'),failed_files_s3_path)
        print('___________________________________________________________________________________________')
        print(f"Some files are not modified. \nPlease check file list here : {failed_files_list_path}")
        print('___________________________________________________________________________________________')
            

#def modified_file_function(json_file_path,file_name,failed_files):
def json_flatten_function(json_file_path,file_name,failed_files,modified_files):
    try:
        #Read json file from folder   
        with open(json_file_path,'rb') as json_file:
            file_data = json_file.read()
            json_file.close()
            
            print('*********************************************')
            print("Reading ",file_name)
            
            #Detect Encoding using chardet Library
            encoding_result = chardet.detect(file_data)
            
            #Retrieve Encoding Information
            encoding = encoding_result['encoding']
            print("Encoding :",encoding)
            print("**********************************************")
            
            #list of encodings
            encoding_to_try = [encoding,'utf-8','latin-1']
            
            #Opening JSON file
            for enc in encoding_to_try:
                try:
                    file = open(json_file_path, encoding=enc)
                    #returns JSON object as {dictionary}
                    data = json.load(file)
                    #print('----------')
                    #print(f"successfully read the file using {enc} encoding")
                    #print('----------')
                    break
                except Exception as e:
                    print('___________________________________________________________________________________________')
                    print(f"Failed to read the file using {enc} encoding.")
                    print('___________________________________________________________________________________________')
                    
            # If the file is empty, raise an error to simulate failure
            if not data:
                raise ValueError(f"No data found in the file : file_Name - {file_name}")

        #Fetch order and customer related data
        order_df = pd.json_normalize(data[0]['Data'])
        order_df = order_df.add_prefix('order_')
        order_df['key'] = 1

        #Shipping related data
        shipto_df = pd.json_normalize(data[0]['Data']['shipto'])
        shipto_df = shipto_df.add_prefix('shipto_')
        shipto_df['key'] = 1

        #Details set data (SKU)
        shipto_details =pd.json_normalize(data[0]['Data']['shipto'],record_path=['detail_set'],record_prefix='s_detail_')
        shipto_details['key'] = 1

        #Payment related data
        payment_df=pd.json_normalize(data[0]['Data'],record_path=['payment'],record_prefix='payment_')
        payment_df['key'] = 1

        #Required columns list
        final_cols_list = ['order_order_number', 'order_order_date', 'order_ship_date','order_attributes.BillingAccountNbr', 'order_attributes.vas_codes', 'order_attributes.hold_vas', 'order_attributes.customer_order_number', 'order_attributes.order_type', 'order_attributes.fulfillment_status', 'order_attributes.business_unit', 'order_attributes.merchandizing_department', 'order_attributes.billing_method_code', 'order_attributes.business_partner_id', 'order_attributes.production_schedule_ref_nbr', 'order_attributes.sales_rep1_employee_id', 'order_attributes.reference_field_2', 'order_attributes.priority_code', 'order_attributes.external_system_purchase_order_number', 'order_attributes.sales_order_number', 'order_attributes.is_back_ordered', 'order_attributes.customer_code', 'order_attributes.origin_facility_alias_id', 'order_attributes.pickup_start_date', 'order_attributes.pickup_end_date', 'order_attributes.delivery_start_datetime', 'order_attributes.delivery_end_datetime', 'order_attributes.cubing_indicator', 'order_attributes.ship_group_id', 'order_attributes.bill_of_lading_nbr', 'order_attributes.international_goods_description', 'order_attributes.route_to', 'order_attributes.route_type1', 'order_attributes.nbr_of_packing_slips_printed', 'order_attributes.mark_for', 'order_attributes.address_code', 'order_attributes.advertising_code', 'order_attributes.scheduled_delivery_end_date', 'order_attributes.is_customer_pickup', 'order_attributes.is_direct_allowed', 'order_attributes.manifest_nbr', 'order_attributes.order_consolidation_profile', 'order_attributes.distribution_order_type', 'order_attributes.routing_attribute', 'order_attributes.dsg_ship_via', 'order_attributes.partial_ship_confirm_flag', 'order_attributes.allow_pre_billing', 'order_attributes.commodity_code', 'order_attributes.destination_action', 'order_attributes.lpn_label_type', 'order_attributes.nbr_of_shipping_labels_to_print', 'order_attributes.content_label_type', 'order_attributes.nbr_of_content_labels_to_print', 'order_attributes.packing_slip_type', 'order_attributes.freight_charge', 'order_attributes.special_instruction_code1', 'order_attributes.special_instruction_code2', 'order_attributes.special_instruction_code3', 'order_attributes.special_instruction_code4', 'order_attributes.special_instruction_code8', 'order_attributes.source_order', 'order_attributes.major_order_group_attr','order_cancel_date', 'order_po_number', 'order_source_code', 'order_customer.customer_number', 'order_customer.first_name', 'order_customer.address1', 'order_customer.city', 'order_customer.state', 'order_customer.zipcode', 'order_customer.country', 'order_customer.company', 'order_customer.phone_number','shipto_shipto_number', 'shipto_first_name', 'shipto_ship_code', 'shipto_shipping', 'shipto_tax_percentage', 'shipto_shipping_tax', 'shipto_address.first_name', 'shipto_address.address1', 'shipto_address.city', 'shipto_address.state', 'shipto_address.zipcode', 'shipto_address.country', 'shipto_address.company', 'shipto_address.phone_number', 'shipto_attributes.carrier_account_number','s_detail_sku', 's_detail_qty_ordered', 's_detail_current_price', 's_detail_attributes.jesta_pickslip_line_number', 's_detail_attributes.jesta_so_line_number', 's_detail_attributes.jesta_so_number', 's_detail_attributes.jesta_pick_qty', 's_detail_attributes.jesta_pick_date', 's_detail_attributes.customer_purchase_order_number', 's_detail_attributes.customer_id', 's_detail_attributes.freight_charge', 's_detail_attributes.jesta_pickslip_size_deta_line_number', 's_detail_attributes.dropship_ind', 's_detail_attributes.IsEmbroidery', 's_detail_attributes.major_order_group_attr', 's_detail_attributes.priority_code', 's_detail_attributes.carton_type', 's_detail_attributes.upc_id', 's_detail_attributes.style_category_id', 's_detail_attributes.style_category_group_id', 's_detail_attributes.item_name', 's_detail_attributes.description', 's_detail_attributes.do_line_nbr', 's_detail_attributes.do_line_status', 's_detail_attributes.is_cancelled', 's_detail_attributes.customer_item_nbr', 's_detail_attributes.edi_buyer_style', 's_detail_attributes.reference_field2_unused', 's_detail_attributes.reference_field3_unused', 's_detail_attributes.reference_field4_two_zeroes', 's_detail_attributes.pickslip_size_detail_id_padded', 's_detail_attributes.truncated_pickslip_detail_sequence_id', 's_detail_attributes.truncated_pickslip_detail_sequence_id_padded', 's_detail_attributes.ref8_set_code', 's_detail_attributes.use_packing_slip_type_2', 's_detail_attributes.pick_size_detail_pick_quantity', 's_detail_attributes.reference_number_field2_unused', 's_detail_attributes.standard_bundle_qty', 's_detail_attributes.standard_lpn_qty', 's_detail_attributes.package_type', 's_detail_attributes.vas_process_type', 's_detail_attributes.price', 's_detail_attributes.retail_price', 's_detail_attributes.lpn_type', 's_detail_attributes.purchase_order_nbr', 's_detail_attributes.purchase_order_line_nbr', 's_detail_attributes.external_system_purchase_order_nbr', 's_detail_attributes.external_system_po_line_nbr', 's_detail_attributes.packaged_weight','payment_payment_number', 'payment_first_name', 'payment_last_name', 'payment_card_code', 'payment_token', 'payment_last4', 'payment_authorization', 'payment_address.customer_number', 'payment_address.first_name', 'payment_address.address1', 'payment_address.city', 'payment_address.state', 'payment_address.zipcode', 'payment_address.country', 'payment_address.company', 'payment_address.phone_number','file_name','run_date']

        result_df=pd.merge(pd.merge(pd.merge(shipto_df,shipto_details,on='key'),payment_df,on='key'),order_df,on='key')

        #final_df=result_df.drop(columns=drop_cols,errors='ignore')
        result_df['file_name'] = f'{file_name}'
        result_df['run_date'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        final_df = result_df.reindex(columns=final_cols_list)

        final_df.to_csv(f'{mod_file_path}\\mod_{file_name}.csv',sep='|',index=False)
        #print(f'Modified file has been created : {mod_file_path}\\mod_{file_name}.csv')
        modified_files.append(file_name)
        print('___________________________________________________________________________________________')
    except Exception as e:
        print(f'Failed to modify file {file_name} : {e}')
        failed_files.append(file_name)

        
#Function will copy files from Prod container to Archive container and then delete copied files from the prod container
def copy_source_to_archive(source_files_list):
    #Delete files list
    source_files_list = source_files_list
    deleted_files_list = []
    
    blob_service_client = initialize_azure_client()
    
    az_success_container_name = get_azure_credentials()[2]
    az_archive_container_name = get_azure_credentials()[3]
    
    # copy file from source to destination container
    source_container_client = blob_service_client.get_container_client(az_success_container_name)
    destination_container_client = blob_service_client.get_container_client(az_archive_container_name)
    
    
    #interate through source_files_list
    #for file_name in selected_blob:
    for file_name in source_files_list:
        source_blob = source_container_client.get_blob_client(file_name)
        destination_blob = destination_container_client.get_blob_client(file_name)

        source_blob_url = source_blob.url
        
        #Copy files from source to destination and delete from the source
        try:
            copy_operation = destination_blob.start_copy_from_url(source_blob_url)
            print(f"Copying {file_name} to {az_archive_container_name}")
            #print('---------------------------------')

            #Wait for the copy operation complete
            props = destination_blob.get_blob_properties()
            while props.copy.status == 'pending':
                props = destination_blob.get_blob_properties()

            #If copy successed, delete the source blob
            if props.copy.status == 'success':
                deleted_files_list.append(source_blob.blob_name)
                source_blob.delete_blob() 
                print(f"Deleted file: {file_name} from {az_success_container_name}")
                print('---------------------------------')
            else:
                print(f"Copy operation for {file_name} did not succeed: {props.copy.status}")

        except Exception as e:
            print(f"Failed to copy {file_name}: {e}")
        
    #Verify whichever files are copied from source to archive container that should be deleted from the source
    verify_deleted_files(source_files_list,deleted_files_list) #Need to enable back

def verify_deleted_files(copied_files,deleted_files):
    # Check if selected_blob and deleted_files_list are the same
    copied_blob_sorted = sorted(copied_files)
    deleted_files_list_sorted = sorted(deleted_files)

    if copied_blob_sorted == deleted_files_list_sorted:
        print('___________________________________________________________________________________________')
        print("All Copied files have been successfully deleted.")
        print('___________________________________________________________________________________________')
    else:
        print('--------------------------------------------------------------------------------------------')
        print("Alert! Some files were not deleted successfully.")
        print("Copied files:", selected_blob_sorted)
        print('___________________________________________________________________________________________')
        print("Deleted files:", deleted_files_list_sorted)
        print('___________________________________________________________________________________________')

def combined_mod_csv_files():
    
    #Get the list of all the csv files from the current timestamp folder 
    azure_combined_mod_files = glob.glob(f'{mod_file_path}\\*')
    
    combined_azure_mod_csv = pd.DataFrame()
    
    #Read all the file one by one and create sigle DataFrame
    for file in azure_combined_mod_files:
        df = pd.read_csv(file,sep='|',dtype=str)
        combined_azure_mod_csv = combined_azure_mod_csv._append(df,ignore_index=True)
    
    #Generate combined csv file in current timestamp folder
    combined_azure_mod_csv.to_csv(f'{mod_file_path}\\combined_mod_azure_file_{current_timestamp}.csv',sep='|',index=False)
    
def read_redshift_conf():
    
    # Write the JSON object to a file
    #json_path = f'{script_path}\credentials.json'
    redshift_cred = f'D:\\dstdw\\Azure_blob\\redshift_cred_prod.json'
    
    with open(redshift_cred,'r') as json_file:
        json_data = json.load(json_file)
        
    
    dbname = json_data['DBNAME']
    host = json_data['HOST']
    port = json_data['PORT']
    user = json_data['USER']
    password = json_data['PASSWORD']
    s3_path = json_data['S3PATH']
    
    return dbname,host,port,user,password,s3_path

def redshift_load(redshift_table_count):
    #print('inside ex_sql_file()')
    
    #Get parameters for Redshift connection
    params = read_redshift_conf()
    
    s3_path = params[5]
    #s3_path =s3_path.replace('RUN_DATE',run_date)
    print(f"-----------------------------------------------------------------")
    print("s3 file path: ",s3_path)
    
    print('all well')
    jazz_orders_copy_command = "copy jesta_prod_ft.jazz_orders_info from '" + s3_path + "/" + run_date + "/" + current_timestamp + "/" "' iam_role 'arn:aws:iam::350027143148:role/dst-redshift-access' DATEFORMAT AS 'auto' TIMEFORMAT AS 'auto' IGNOREHEADER AS 1 DELIMITER '|'REMOVEQUOTES escape;"
    print('--------------------------------------------------------------------------------------------')
    print("\n copy command for Jazz Orders Info table is :", jazz_orders_copy_command)

    Total_count = "select count(*) from jesta_prod_ft.jazz_orders_info;"
    print('--------------------------------------------------------------------------------------------')
    print("\n Total count of table is :", Total_count)

    #conn = psycopg2.connect(dbname=params['DBNAME'], host=params['HOST'], port=params['PORT'], user=params['USER'], password=params['PASSWORD'])
    conn = psycopg2.connect(dbname=params[0], host=params[1], port=params[2], user=params[3], password=params[4])
    cur = conn.cursor();
    print('Connection has been established!!')
    
    cur.execute("begin;")
    cur.execute(jazz_orders_copy_command)
    cur.execute("commit;")

    cur.execute("begin;")
    cur.execute(Total_count)
    count_tup = cur.fetchall()[0]
    count = ''.join(map(str,count_tup))
    msg = count + ' Records loaded in jazz_orders_info table'
    print(f"-----------------------------------------------------------------")
    print(msg)
    redshift_table_count.append(msg.split()[0])
    cur.execute("commit;")

    conn.close()
    print("Copy commands for jesta_prod_ft.jazz_orders_info table executed fine!")
    print(f'*********** Redshift load completed : {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} ************')
    print("**************************************************************************")

if __name__ == "__main__":
    print("calling function to get latest blob files to load")
    get_list_of_latest_blobs()
   
