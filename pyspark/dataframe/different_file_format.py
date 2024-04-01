from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Different file formats").getOrCreate()

# CSV
csv_df = spark.read.option("header", "true").csv("C:\\Users\\SathyaPriyaR\\Desktop\\industry.csv", inferSchema=True)
csv_df.show()
print("CSV Schema:")
csv_df.printSchema()

# Parquet
parquet_df = spark.read.parquet("C:\\Users\\SathyaPriyaR\\Downloads\\sample1.parquet")
parquet_df.show()
print("Parquet Schema:")
parquet_df.printSchema()

# JSON
json_df = spark.read.json("C:\\Users\\SathyaPriyaR\\Downloads\\file4.json")
json_df.show()
print("JSON Schema:")
json_df.printSchema()

spark.stop()
