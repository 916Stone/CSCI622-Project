# Databricks notebook source
# Mirai and Okiru are 2 differnt types of malware
# This script is processing the features in the 2 malware datasets
import sys

# COMMAND ----------

# connect to storage account
storage_end_point = "622project.dfs.core.windows.net" 
my_scope = "CSCI622Scope"
my_key = "project-key"

spark.conf.set(
    "fs.azure.account.key." + storage_end_point,
    dbutils.secrets.get(scope=my_scope, key=my_key))

# Replace the container name and storage account name in the uri.
uri = "abfss://project@622project.dfs.core.windows.net/"

# COMMAND ----------

# Read the data file from the storage account. 
try:
    mirai_df = spark.read.csv(uri+'upload-data/mirai.csv', header=True)
    print("mirai.csv has been loaded.")

    okiru_df = spark.read.csv(uri+'upload-data/okiru.csv', header=True)
    print("okiru.csv has been loaded.")

except Exception as e:
    print("LoadingError:")
    print(e)

# Basic info
print(f"Mirai rows: {mirai_df.count()}, columns: {len(mirai_df.columns)}")
print(f"Okiru rows: {okiru_df.count()}, columns: {len(okiru_df.columns)}")

# COMMAND ----------

# Some data validation
columns_to_check = [
    'ts', 'uid', 'id.orig_h', 'id.orig_p', 'id.resp_h', 'id.resp_p', 
    'proto', 'service', 'duration', 'orig_bytes', 'resp_bytes', 
    'conn_state', 'local_orig', 'local_resp', 'missed_bytes', 
    'history', 'orig_pkts', 'orig_ip_bytes', 'resp_pkts', 
    'resp_ip_bytes', 'tunnel_parents', 'label', 'detailed-label'
]

if all(column in mirai_df.columns for column in columns_to_check) and all(column in okiru_df.columns for column in columns_to_check):
    print("Both mirai_df and okiru_df contain all the specified columns.")
else:
    print("One or both DataFrames are missing some of the specified columns.")

# COMMAND ----------

# Convert Benign to 0 and malicious to 1 for mirai 2 for okiru before combine the dataframe
from pyspark.sql.functions import when, col, count

mirai_df = mirai_df.withColumn("label", when(col("label") == "Benign", 0).otherwise(1))
mirai_count = mirai_df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False)
okiru_df = okiru_df.withColumn("label", when(col("label") == "Benign", 0).otherwise(2))
okiru_count = okiru_df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False)

mirai_count.show()
okiru_count.show()

# COMMAND ----------

# Remove column tunnel_parents and detailed-label because they are not used for this analysis
columns_to_remove = ['tunnel_parents', 'detailed-label']

if all(column in mirai_df.columns for column in columns_to_remove) and all(column in okiru_df.columns for column in columns_to_remove):
    mirai_df = mirai_df.drop(*columns_to_remove)
    okiru_df = okiru_df.drop(*columns_to_remove)

print(mirai_df.columns)

# COMMAND ----------

# Combine 2 dataframes so they will be processed as one to make sure consistensy in features

# Validate if the columns are the same before combine
if(mirai_df.columns == okiru_df.columns): 
    df = mirai_df.union(okiru_df)
else:
    print("Columns are different in dataframes.")
    sys.exit("Terminating the script.")

# Display new labels    
df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False).show()

# COMMAND ----------

# Save the merged dataset
try:
    # Save the merged dataset
    print("Saving merged file...")
    df.write.mode('overwrite').csv(uri+"process-data/merged.csv", header=True)
    print("File saved in storage under folder process-data.")
    
except Exception as e:
    print("SavingError:")
    print(e)
