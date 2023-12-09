# DataSet2.py - script to extract data from its source and load into ADLS.

print("DataSet2 ingestion")

# Import libraries
from azure.storage.filedatalake import DataLakeServiceClient
import wget
import os
import pandas as pd

# Connect to ADLS
def initialize_storage_account(storage_account_name, storage_account_key):
    
    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)
    
    except Exception as e:
        print(e)

storage_account_name = "622project"

with open("src\ingestion\AccountKey.config") as f:
    storage_account_key=f.readline()

initialize_storage_account(storage_account_name, storage_account_key)
print("ADLS connection established.")

# This labeled data can be treated as csv file, it does need a few more tweaks before transforming it into a dataframe, the processing will be in EDA.
# Download data using wget
print("Downloading IoT-23 dataset from CTU...")
url = "https://mcfp.felk.cvut.cz/publicDatasets/IoT-23-Dataset/IndividualScenarios/CTU-IoT-Malware-Capture-35-1/bro/conn.log.labeled"
directory = "data/iot-23/"
filename = wget.download(url, out=directory)
print("Download complete.")

# Transform to dataframe before loading to ADLS
df = pd.read_csv("data/iot-23/conn.log.labeled", comment="#", sep='\t| ', engine='python')

print("Transforming IoT-23 dataset to dataframe...")
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

# Add column names ts	uid	id.orig_h	id.orig_p	id.resp_h	id.resp_p	proto	service	duration	orig_bytes	resp_bytes	conn_state	local_orig	local_resp	missed_bytes	history	orig_pkts	orig_ip_bytes	resp_pkts	resp_ip_bytes	tunnel_parents   label   detailed-label
df.columns = ['ts', 'uid', 'id.orig_h', 'id.orig_p', 'id.resp_h', 'id.resp_p', 'proto', 
              'service', 'duration', 'orig_bytes', 'resp_bytes', 'conn_state', 
              'local_orig', 'local_resp', 'missed_bytes', 'history', 'orig_pkts', 
              'orig_ip_bytes', 'resp_pkts', 'resp_ip_bytes', 'tunnel_parents', 'label', 'detailed-label']

# Save to csv on local
df.to_csv("data/iot-23/conn_log.csv", index=False)

# Load data into ADLS
print("Uploading IoT-23 dataset to ADLS...")
try:
    file_system_client = service_client.get_file_system_client(file_system="project")

    directory_client = file_system_client.get_directory_client("upload-data")
        
    file_client = directory_client.create_file("conn_log.csv")

    # Use contect manager to avoid permission error
    with open("data/iot-23/conn_log.csv", 'r') as population_file:
        file_contents = population_file.read()
        file_client.upload_data(file_contents, overwrite=True)

    print("IoT-23 dataset uploaded to ADLS.")

except Exception as e:
    print(e)

# Remove downloaded data
print("Removing downloaded data...")
import shutil
shutil.rmtree("src/ingestion/data/")
print("Downloaded data removed.")

