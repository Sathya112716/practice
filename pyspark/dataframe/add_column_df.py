from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
spark = SparkSession.builder.appName("Add Columns Example").getOrCreate()

data = [("John", 25), ("Alice", 30), ("Bob", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

print("Original DataFrame:")
df.show()

# Add a new column "City"
df_with_city = df.withColumn("City",lit("New York"))

from pyspark.sql.functions import when
df_with_gender = df_with_city.withColumn("Gender", when(df["Name"] == "John", "Male").otherwise("Female"))

print("DataFrame with added columns:")
df_with_gender.show()

spark.stop()
