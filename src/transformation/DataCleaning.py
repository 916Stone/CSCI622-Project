# Databricks notebook source
# This script clean the data by handeling missing values and deal with single value feaures
# It's short script but PySpark is much slower dealing with these compared to Pandas, so this script might take a while to run

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

# Read the merged file from the storage account. 
from pyspark.sql.functions import when, col, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType

schema = StructType([StructField("ts", DoubleType(), True), \
                    StructField("uid", StringType(), True), \
                    StructField("idOrigH", StringType(), True), \
                    StructField("idOrigPort", IntegerType(), True), \
                    StructField("idRespH", StringType(), True), \
                    StructField("idRespPort", IntegerType(), True), \
                    StructField("proto", StringType(), True), \
                    StructField("service", StringType(), True), \
                    StructField("duration", DoubleType(), True), \
                    StructField("origBytes", IntegerType(), True), \
                    StructField("respBytes", IntegerType(), True), \
                    StructField("connState", StringType(), True), \
                    StructField("localOrig", StringType(), True), \
                    StructField("localResp", StringType(), True), \
                    StructField("missedBytes", IntegerType(), True), \
                    StructField("history", StringType(), True), \
                    StructField("origPkts", IntegerType(), True), \
                    StructField("origIpBytes", IntegerType(), True), \
                    StructField("respPkts", IntegerType(), True), \
                    StructField("respIpBytes", IntegerType(), True), \
                    StructField("label", IntegerType(), True), \
                    ])

try:
    df = spark.read.schema(schema).csv(uri+'process-data/merged.csv', header=True)

    # Timestamp and id are not used for analysis, but they will be saved just in case 
    df_meta = df.select('ts', 'uid')
    df = df.drop('ts', 'uid')
    print("Merged data loaded.")
except Exception as e:
    print("LoadingError:")
    print(e)

# COMMAND ----------

# Validate the date, check if there are 3 labels
labels = [0, 1, 2]
if all([df.where(col("label") == label).count() > 0 for label in labels]):
    df.groupBy("label").agg(count("label").alias("count")).orderBy("count", ascending=False).show()
else:
    print("Dataframe does not contain all 3 labels.")

# COMMAND ----------

# Check the columns that only has one distinct value
# Use this function to limit the count that significantly increase the speed 
def check_distinct(column, df, num_of_distinct):
    return df.select(f"`{column}`").distinct().limit(num_of_distinct+1).count() == num_of_distinct

columns_single_value = [column for column in df.columns if check_distinct(column, df, 1)]

print(f'Removed columns {columns_single_value} that contains only 1 value.')
df_distinct = df.drop(*columns_single_value)

# COMMAND ----------

# Handel null values
# In the original dataset, the missing value is represented as -
from pyspark.sql.functions import lit

columns_high_missing = []

for column in df_distinct.columns:
    # Calculate the frequency of '-' in the current column
    count = df_distinct.filter(col(column) == '-').count()
    frequency = (count / df_distinct.count()) * 100

    # Check if this frequency is greater than 90%
    if frequency > 90:
        columns_high_missing.append(column)

print(f'Removed columns {columns_high_missing} that contains more than 90% of missing value.')
df_distinct = df_distinct.drop(*columns_high_missing)

# COMMAND ----------

# Becasue the training data will be mainly benign, so the missing value percentage will also be based on the benign samples
benign = df_distinct.filter(col("label") == 0)

benign_count = benign.count()
columns_null = [c for c in benign.columns if benign.select([(count(when(col(c).isNull(), c))/benign_count).alias(c)]).first()[c] > 0.8]

print(f'Removed columns {columns_null} that contains more than 80% missing value for benign samples.')
df_cleaned = df_distinct.drop(*columns_null)

# COMMAND ----------

df_cleaned.printSchema()

# COMMAND ----------

# Save the cleaned dataset
try:
    # Save the cleaned dataset
    print("Saving cleaned file...")
    df_cleaned.write.mode('overwrite').csv(uri+"process-data/cleaned.csv", header=True)
    print("File saved in storage under folder process-data.")
    
except Exception as e:
    print("SavingError:")
    print(e)
