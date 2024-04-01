from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import functions as F
spark = SparkSession.builder.appName("Numeric_Functions_and_Operations").getOrCreate()

data = [(1, 2, 3), (-4, 5, -6), (7, -8, 9)]
columns = ["data1", "data2", "data3"]
df = spark.createDataFrame(data, columns)

print("Original DataFrame:")
df.show()

print("Sum of data1:", df.select(sum("data1")).collect()[0][0])
print("Average of data2:", df.select(avg("data2")).collect()[0][0])
print("Minimum of data3:", df.select(min("data3")).collect()[0][0])
print("Maximum of data1:", df.select(max("data1")).collect()[0][0])

rounded_df = df.withColumn("data1_rounded", round("data1", 1))
print("DataFrame with rounded col1:")
rounded_df.show()

abs_df = df.withColumn("data2_abs", abs("data2"))
print("DataFrame with absolute values of data2:")
abs_df.show()

print("Logical AND between data1 and data2:")
df.select("data1", "data2", F.expr("data1 > 0 AND data2 > 0")).show()
print("Logical OR between col1 and col2:")
df.select("data1", "data2", F.expr("data1 > 0 OR data2 > 0")).show()

# Stop the SparkSession
spark.stop()
