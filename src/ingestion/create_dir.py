# %%
from azure.storage.filedatalake import DataLakeServiceClient

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

print(service_client)

# %%
# create container and directory and catch exception if already exists
try:
    file_system_client = service_client.create_file_system(file_system="project")

    directory_client=file_system_client.create_directory("data")

    directory_client.create_directory()

except Exception as e:
    print(e)


