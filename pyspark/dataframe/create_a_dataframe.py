from pyspark.sql import SparkSession
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder.appName("DataFrame Creation").getOrCreate()

# Method 1: Create DataFrame from an existing collection
data = [("Sathya", 24), ("Eniyan", 16), ("Ambika", 50)]
df1 = spark.createDataFrame(data, ["Name", "Age"])
print("DataFrame 1:")
df1.show()

spark.stop()
