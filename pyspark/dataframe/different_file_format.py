from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder.appName("Different file formats").getOrCreate()
sc = spark.sparkContext
csv_schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", IntegerType(), True)
])

parquet_schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", IntegerType(), True)
])

json_schema = StructType([
    StructField("col1", StringType(), True),
    StructField("col2", IntegerType(), True)
])

#csv
csv_df = spark.read.csv("C:\\Users\\SathyaPriyaR\\Desktop\\industry.csv", schema=csv_schema)
csv_df.show()
#parquet
parquet_df = spark.read.parquet("C:\\Users\\SathyaPriyaR\\Downloads\\part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet", schema=parquet_schema)

#json
json_df = spark.read.json("C:\\Users\\SathyaPriyaR\\Downloads\\file4.json", schema=json_schema)

print("CSV Schema:")
csv_df.printSchema()

print("Parquet Schema:")
parquet_df.printSchema()

print("JSON Schema:")
json_df.printSchema()

spark.stop()