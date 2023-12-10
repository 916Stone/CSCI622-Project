# Databricks notebook source
# This script focus on extract features from the categorical features

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DoubleType, LongType

# Clear cache before reading
spark.catalog.clearCache()

try:
    df = spark.read.csv(uri+'process-data/cleaned.csv', header=True, inferSchema=True)
    print("Cleaned has been loaded.")

except Exception as e:
    print("LoadingError:")
    print(e)

df.printSchema()

# COMMAND ----------

# Extract port number features
port  = [c for c in df.columns if 'Port' in c]

print(port)

# COMMAND ----------

# Labeling port numbers
# well-known ports (0-1023), registered ports (1024-49151), and dynamic/private ports (49152-65535)
def label_port(port):
    if 0 <= port <= 1023:
        return 'wk'
    elif 1024 <= port <= 49151:
        return 'reg'
    elif 49152 <= port <= 65535:
        return 'dyn'
    else:
        return 'unk'

label_port_udf = udf(label_port, StringType())

for p in port:
    df = df.withColumn(p, label_port_udf(df[p]))
    df.select(p).distinct().show()

# COMMAND ----------

# Get the ip address columns
ip_addr = [c for c in df.columns if 'H' in c]

print(ip_addr)

# COMMAND ----------

# Split the IP addresses to 4 numbers
from pyspark.sql.functions import split

for ip in ip_addr:
    # Replace IPv6 with '0.0.0.0' and then split by '.'
    df = df.withColumn(ip, when(col(ip).contains(':'), '0.0.0.0').otherwise(col(ip)))
    split_col = split(df[ip], '\\.')
    for i in range(4):
        df = df.withColumn(f'{ip}_{i+1}', split_col.getItem(i).cast(IntegerType()))

df = df.drop(*ip_addr)
df.printSchema()

# COMMAND ----------

# Separate categorical and numerical features
numerical = []
categorical = []

for field in df.schema.fields:
    if isinstance(field.dataType, (IntegerType, LongType, DoubleType, FloatType)):
        numerical.append(field.name)
    elif isinstance(field.dataType, StringType):
        categorical.append(field.name)

print(numerical)
print(categorical)

# COMMAND ----------

# Get the categorical feature that have more than 5 labels, it would creates too many dimention when encoding
def columns_with_min_distinct_values(df, columns, min_distinct_count):
    columns_with_min_count = []
    for column in columns:
        distinct_count = df.select(column).distinct().count()
        if distinct_count > min_distinct_count:
            columns_with_min_count.append(column)
    return columns_with_min_count

categorical_5 = columns_with_min_distinct_values(df, categorical, 5)
print(categorical_5)

# COMMAND ----------

# Validate the columns left are connState and history, otherwise there are something wrong with previous steps
import sys

if sorted(categorical_5) != sorted(['connState', 'history']):
    print("Processing error.")
    sys.exit(1)

# COMMAND ----------

# To deal with connection state and history, some field knowledge is required, but the basic idea  is to combine labels for certain condition
# A detailed infomation can be found on Github
from pyspark.sql.functions import regexp_replace

# It is slightly easier to deal with connState because there are limit amount of labels, history however is a combination of strings

# Combine all labels contains RST, SH, and then combine SF to S0
df = df.withColumn('connState', 
                   when(df['connState'].rlike('RST'), 'RST')
                   .when(df['connState'].rlike('SH'), 'SH')
                   .when(df['connState'] == 'SF', 'S0')
                   .otherwise(df['connState']))

# Combine lables contain SH, S1, and S2 to N
df = df.withColumn('connState', when(df['connState'].rlike('SH|S1|S2'), 'N').otherwise(df['connState']))

# Print the distinct values of the 'conn_state' column
df.select('connState').distinct().show()

# COMMAND ----------

# Validate the columns that has more than 5 labels again
def columns_with_min_distinct_values(df, columns, min_distinct_count):
    columns_with_min_count = []
    for column in columns:
        distinct_count = df.select(column).distinct().count()
        if distinct_count > min_distinct_count:
            columns_with_min_count.append(column)
    return columns_with_min_count

categorical_5 = columns_with_min_distinct_values(df, categorical, 5)

if sorted(categorical_5) != sorted(['history']):
    print("Processing error.")
    sys.exit(1)
else:
    print(categorical_5)

# COMMAND ----------

# History column is a set of string that can have different combinations, length, and order
# However, this letters does all have meanings, so they cannot be encoded using integars or frequencies
# A properway of doing this is to combine these labels based on what letter they contain
# But the order matters in this case becasue a string can contain multiple letters

benign_flags = ['s', 'a', 'd', 'f', 'S', 'A', 'D', 'F', 'S']
malicious_flags = ['r', 'i', 'q', '^', 'R', 'I', 'Q']

def combine_labels_by_order(value, flags):
    for flag in flags:
        if flag in value:
            return flag
    return value  # Return the original value if no flag is found

def combine_labels(df, column, flags):
    categorize_udf = udf(lambda value: combine_labels_by_order(value, flags), StringType())
    return df.withColumn(column, categorize_udf(df[column]))

# Combine labels that contains these letters that normally represents malicious behavior
df = combine_labels(df, 'history', malicious_flags)

# COMMAND ----------

# If there are still mor than 5 labels then combine labels that contains these letters that normally represents benign behavior
if (df.select('history').distinct().count() > 5):
    df = combine_labels(df, 'history', benign_flags)

print(df.select('history').distinct().count())

# COMMAND ----------

# Get the frequency of the values in the column
def calculate_frequency(df, column):
    total_count = df.count()
    frequency_df = df.groupBy(column).count()
    frequency_df = frequency_df.withColumn('frequency', (col('count') / total_count) * 100)
    return frequency_df

# Combine lables based on low frequency
def combine_based_on_frequency(df, column_name, frequency_df, threshold, benign_flags, malicious_flags):
    low_freq_values = frequency_df.filter(col('frequency') < threshold).select(column_name).rdd.flatMap(lambda x: x).collect()

    def new_category(value):
        if value in low_freq_values:
            if value in benign_flags:
                return 'B'
            elif value in malicious_flags:
                return 'M'
            # else:
            #     return 'Other'
        else:
            return value

    new_category_udf = udf(new_category, StringType())
    return df.withColumn(column_name, new_category_udf(col(column_name)))

# COMMAND ----------

# Call the function on the list of thresholds untill there are 5 labels or less
thresholds = [0.01, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5]
column_name = 'history' 

for threshold in thresholds:
    # Calculate the frequency
    frequency_df = calculate_frequency(df, column_name)
    
    # Combine based on frequency
    df = combine_based_on_frequency(df, column_name, frequency_df, threshold, benign_flags, malicious_flags)

    # Check the distinct value count
    distinct_count = df.select(column_name).distinct().count()

    if distinct_count <= 5:
        print(f'Combined label that has less than {threshold}% frequency')
        df.groupBy(col(column_name)).count().orderBy(col('count').desc()).show()
        break

# If the labels are still more than 5
if distinct_count > 5:
    print("The count of distinct values is still greater than 5 after applying all thresholds.")
    sys.exit(1)
    # A prompt would be appropriate in here but I am not sure if a prompt would affect automation


# COMMAND ----------

# fill the missing values as NA
df = df.fillna({"history": "NA"})

# COMMAND ----------

# Validate again before saving
def columns_with_min_distinct_values(df, columns, min_distinct_count):
    columns_with_min_count = []
    for column in columns:
        distinct_count = df.select(column).distinct().count()
        if distinct_count > min_distinct_count:
            columns_with_min_count.append(column)
    return columns_with_min_count

categorical_5 = columns_with_min_distinct_values(df, categorical, 5)

if not categorical_5:
    print("Saving engineered file...")
else:
    print(f'{categorical_5} columns still contain more than 5 labels')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Save the engineered dataset
try:
    # Save the engineered dataset
    df.write.mode('overwrite').csv(uri+"process-data/feature.csv", header=True)
    print("File saved in storage under folder process-data.")
    
except Exception as e:
    print("SavingError:")
    print(e)
