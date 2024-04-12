# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime

# Create SparkSession
spark = SparkSession.builder \
    .appName("SCD Type Implementation") \
    .getOrCreate()

# Define dimension
data = [
    (1, 'Sathya', '2022-01-01', '2022-02-28'),
    (2, 'Priya', '2022-01-01', '2022-02-28'),
    (3, 'Sandhya', '2022-01-01', '2022-02-28')
]

# Define schema for dimension data
schema = ["id", "name", "valid_from", "valid_to"]

# Create DataFrame for dimension data
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()


# COMMAND ----------

# Write DataFrame to a Delta table in DBFS
df.write.format("delta").mode("overwrite").save("dbfs:/path/to/delta_table_scd0")


# COMMAND ----------

# Assume df_updated is the DataFrame containing the updated dimension data
data_updated = [
    (1, 'Sathya', '2022-01-01', '2022-03-31'),  # Updated record
    (2, 'Priya', '2022-01-01', '2022-02-28'),
    (3, 'Sandhya', '2022-01-01', '2022-02-28')
]

df_updated = spark.createDataFrame(data_updated, schema)

# Write updated DataFrame to a Delta table in DBFS
df_updated.write.format("delta").mode("overwrite").save("dbfs:/path/to/delta_table_scd1")


# COMMAND ----------

# Assume df_updated is the DataFrame containing both new and updated dimension data
data_updated = [
    (1, 'Sathya', '2022-01-01', '2022-03-31'),  # Updated record
    (2, 'Priya', '2022-01-01', '2022-02-28'),
    (3, 'Sandhya', '2022-01-01', '2022-02-28'),
    (4, 'Ravi', '2022-03-01', '2022-03-31')  # New record
]

df_updated = spark.createDataFrame(data_updated, schema)

# Write updated DataFrame to a Delta table in DBFS
df_updated.write.format("delta").mode("overwrite").save("dbfs:/path/to/delta_table_scd2")

