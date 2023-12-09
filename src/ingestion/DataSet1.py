# DataSet1.py - script to extract data from its source and load into ADLS.

print("DataSet1 ingestion")

# Import libraries
from azure.storage.filedatalake import DataLakeServiceClient
from kaggle.api.kaggle_api_extended import KaggleApi
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

# Connect Kaggle API
api = KaggleApi()
api.authenticate()

# Download data from Kaggle
print("Downloading Edge-IIoT dataset from Kaggle...")
api.dataset_list_files('mohamedamineferrag/edgeiiotset-cyber-security-dataset-of-iot-iiot')
api.dataset_download_files('mohamedamineferrag/edgeiiotset-cyber-security-dataset-of-iot-iiot', 'src/ingestion/data/iiot', unzip=True)
print("Download complete.")

# Load data into ADLS
print("Uploading IoT-23 dataset to ADLS...")

try:
    file_system_client = service_client.get_file_system_client(file_system="project")

    directory_client = file_system_client.get_directory_client("upload-data")
        
    file_client = directory_client.create_file("iiot.csv")

    # Use contect manager to avoid permission error
    with open("src/ingestion/data/iiot/Edge-IIoTset dataset/Selected dataset for ML and DL/DNN-EdgeIIoT-dataset.csv", 'r') as population_file:
        file_contents = population_file.read()
        file_client.upload_data(file_contents, overwrite=True)

    print("IIoT dataset uploaded to ADLS.")

except Exception as e:
    print(e)

# Remove downloaded data
print("Removing downloaded data...")
import shutil
shutil.rmtree('src/ingestion/data/iiot')
print("Downloaded data removed.")