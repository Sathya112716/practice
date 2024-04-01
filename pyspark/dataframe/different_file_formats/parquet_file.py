from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Parquet file formats").getOrCreate()


# Read the Parquet file
parquet_df = spark.read.parquet("C:\\Users\\SathyaPriyaR\\Desktop\\pyspark\\practice\\pyspark\\resources\\part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")

parquet_df.show()
parquet_df.printSchema()
