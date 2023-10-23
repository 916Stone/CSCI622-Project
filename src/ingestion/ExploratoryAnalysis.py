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

# Read another data
iot23_df = spark.read.csv(uri+'upload-data/conn_log.csv', header="True")
 
display(iot23_df)

# COMMAND ----------

# Basic info of this df
print(f"Number of rows: {iot23_df.count()}")
print(f"Number of columns: {len(iot23_df.columns)}")

print(iot23_df.printSchema())

# COMMAND ----------

# The last column is the label for the type of malware which will probably not be used
iot23_df.select("detailed-label").distinct().show()

# COMMAND ----------

# Check the target values again
iot23_count = iot23_df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False)

iot23_count.show()

# COMMAND ----------

# Convert Benign to 0 and other types to 1 and plot the count  
iot23_df = iot23_df.withColumn("label", when(col("label") == "Benign", 0).otherwise(1))

iot23_count = iot23_df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False)

iot23_count.show()

# COMMAND ----------

# Count plot 
# Extracting values and counts
values = [row['label'] for row in iot23_count.collect()]
counts = [row['count'] for row in iot23_count.collect()]

# Plotting
colors = plt.cm.viridis(np.linspace(0, 1, len(values)))
plt.bar(values, counts, color=colors)
plt.xlabel('Attack_Type')
plt.ylabel('Count')
plt.title('Attack_Type Counts')
plt.xticks(values)
plt.show()
