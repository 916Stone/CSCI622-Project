# Databricks notebook source
# ExploratoryAnalysis.py - script to do EDA (exploratory data analysis) on the two primary data sets for the project. 

# Note - alternative approaches can be used besides local Python code.  Large datasets may require Databricks.  
# You can also use ChatGPT or similar language model for this step. 

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
iiot_df = spark.read.csv(uri+'upload-data/iiot.csv', header=True)
 
display(iiot_df)

# COMMAND ----------

# Basic info of this df
print(f"Number of rows: {iiot_df.count()}")
print(f"Number of columns: {len(iiot_df.columns)}")

print(iiot_df.printSchema())

# COMMAND ----------

# Look at the values on the target column
from pyspark.sql.functions import count

iiot_df.groupBy("Attack_Type").agg(count("Attack_Type").alias("count")).orderBy("count", ascending=False).show()

# COMMAND ----------

# Convert Normal to 0 and other types to 1   
from pyspark.sql.functions import when, col
iiot_df = iiot_df.withColumn("Attack_Type", when(col("Attack_Type") == "Normal", 0).otherwise(1))

# COMMAND ----------

# Check the target values again
iiot_count = iiot_df.groupBy("Attack_Type").agg(count("Attack_Type").alias("count")).orderBy("count", ascending=False)

iiot_count.show()

# COMMAND ----------

# Count plot for the target column
import matplotlib.pyplot as plt
import numpy as np

# Extracting values and counts
values = [row['Attack_Type'] for row in iiot_count.collect()]
counts = [row['count'] for row in iiot_count.collect()]

# Plotting
colors = plt.cm.viridis(np.linspace(0, 1, len(values)))
plt.bar(values, counts, color=colors)
plt.xlabel('Attack_Type')
plt.ylabel('Count')
plt.title('Attack_Type Counts')
plt.xticks(values)
plt.show()

# COMMAND ----------

# Read mirai dataset
mirai_df = spark.read.csv(uri+'upload-data/mirai.csv', header="True")
 
display(mirai_df)

# COMMAND ----------

# Basic info of this df
print(mirai_df.printSchema())

# COMMAND ----------

# The last column is the label for the type of malware which will probably not be used
mirai_df.select("detailed-label").distinct().show()

# COMMAND ----------

# Check the target values again
mirai_count = mirai_df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False)

mirai_count.show()

# COMMAND ----------

# Convert Benign to 0 and other types to 1 and plot the count  
mirai_df = mirai_df.withColumn("label", when(col("label") == "Benign", 0).otherwise(1))

mirai_count = mirai_df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False)

mirai_count.show()

# COMMAND ----------

# Count plot 
# Extracting values and counts
values = [row['label'] for row in mirai_count.collect()]
counts = [row['count'] for row in mirai_count.collect()]

# Plotting
colors = plt.cm.viridis(np.linspace(0, 1, len(values)))
plt.bar(values, counts, color=colors)
plt.xlabel('Attack_Type')
plt.ylabel('Count')
plt.title('Attack_Type Counts')
plt.xticks(values)
plt.show()

# COMMAND ----------

# Read okiru dataset
okiru_df = spark.read.csv(uri+'upload-data/okiru.csv', header="True")
 
display(okiru_df)

# COMMAND ----------

# Check the target values again
okiru_count = okiru_df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False)

okiru_count.show()

# COMMAND ----------

# Convert Benign to 0 and other types to 1 and plot the count  
okiru_df = okiru_df.withColumn("label", when(col("label") == "Benign", 0).otherwise(1))

okiru_count = okiru_df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False)

okiru_count.show()

# COMMAND ----------

# Count plot 
# Extracting values and counts
values = [row['label'] for row in okiru_count.collect()]
counts = [row['count'] for row in okiru_count.collect()]

# Plotting
colors = plt.cm.viridis(np.linspace(0, 1, len(values)))
plt.bar(values, counts, color=colors)
plt.xlabel('Attack_Type')
plt.ylabel('Count')
plt.title('Attack_Type Counts')
plt.xticks(values)
plt.show()
