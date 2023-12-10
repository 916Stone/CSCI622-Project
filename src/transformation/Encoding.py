# Databricks notebook source
# This script encode the categorical features to numerical and validate last time before serving
# The final validation could also take a while to run

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Clear cache before reading
spark.catalog.clearCache()

try:
    df = spark.read.csv(uri+'process-data/feature.csv', header=True, inferSchema=True)
    print("Cleaned has been loaded.")

except Exception as e:
    print("LoadingError:")
    print(e)

# COMMAND ----------

# Validation
import sys

for field in df.schema.fields:
    if not isinstance(field.dataType, (StringType, IntegerType)):
        print("The DataFrame contains data types other than String and Integer.")
        sys.exit(1)

# COMMAND ----------

from pyspark.ml.feature import StringIndexer, OneHotEncoder
from pyspark.ml import Pipeline

# Extract string columns
string_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

# OneHotEncoder
stages = []
for string_col in string_columns:
    indexer = StringIndexer(inputCol=string_col, outputCol=string_col + '_indexed')
    encoder = OneHotEncoder(inputCols=[indexer.getOutputCol()], outputCols=[string_col + '_encoded'])
    stages += [indexer, encoder]

# Create and Run Pipeline
pipeline = Pipeline(stages=stages)
df_transformed = pipeline.fit(df).transform(df)

df_transformed = df_transformed.drop(*string_columns)

# COMMAND ----------

from pyspark.sql.functions import udf, col
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as F

# UDF to extract vector element and convert to integer
def extract_vector_element(vector, index):
    try:
        return int(vector[index])
    except IndexError:
        return 0

# Register UDF with IntegerType
extract_vector_element_udf = udf(extract_vector_element, IntegerType())

# List of new column names
new_column_names = []

for string_col in string_columns:
    indexed_col = string_col + '_indexed'  # Name of the indexed column
    encoded_col = string_col + '_encoded'  # Name of the encoded column
    num_categories = df_transformed.select(encoded_col).head()[0].size

    # Extract and cast elements as integers
    for i in range(num_categories):
        new_col_name = f"{string_col}_category_{i}"
        new_column_names.append(new_col_name)
        df_transformed = df_transformed.withColumn(new_col_name, extract_vector_element_udf(F.col(encoded_col), F.lit(i)).cast(IntegerType()))

# Remove the original vector columns and indexed columns
columns_to_drop = [col + '_encoded' for col in string_columns] + [col + '_indexed' for col in string_columns]
df_transformed = df_transformed.drop(*columns_to_drop)

# COMMAND ----------

from pyspark.sql.functions import isnan, when, count
df = df.dropDuplicates()

# Final validation
# For some reason the validation for the schema takes a very long time to run
def validate_dataframe(df):
    # Check if all columns are of integer type first
    non_integer_columns = [f.name for f in df.schema.fields if f.dataType != IntegerType()]
    if non_integer_columns:
        print("Not all columns are of integer type. Non-integer columns: ", non_integer_columns)
        raise Exception("DataFrame contains non-integer columns.")

    # Check for missing values in each column
    missing_values_count = df.select([count(when(col(c).isNull() | isnan(c), c)).alias(c) for c in df.columns]).collect()
    missing_values_dict = missing_values_count[0].asDict()

    # Check if there are any missing values
    if any(missing_count > 0 for missing_count in missing_values_dict.values()):
        print("Missing values found in the DataFrame. Process terminated.")
        raise Exception("DataFrame contains missing values.")

validate_dataframe(df_transformed)

# COMMAND ----------

# Save the cleaned dataset
try:
    # Save the cleaned dataset
    print("Saving encoded file...")
    df_transformed.write.mode('overwrite').csv(uri+"process-data/encoded.csv", header=True)
    print("File saved in storage under folder process-data.")
    
except Exception as e:
    print("SavingError:")
    print(e)
