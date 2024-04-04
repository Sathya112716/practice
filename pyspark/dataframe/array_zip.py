from pyspark.sql import SparkSession
from pyspark.sql.functions import col, arrays_zip

# Initialize SparkSession
spark = SparkSession.builder.appName("Array Zip").getOrCreate()


df = spark.read.option("mode", "PERMISSIVE").json("C:\\Users\\SathyaPriyaR\\Desktop\\pyspark\\practice\\pyspark\\resources\\sample 2 for practice.json")

df.show()

zipped_df = df.withColumn("zipped_data", arrays_zip(col("phoneNumber"), col("email"), col("hexcolor")))

# Show zipped data
zipped_df.show(truncate=False)

# Stop SparkSession
spark.stop()
