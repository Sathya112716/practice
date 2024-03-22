from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import lit, when
# Create a new SparkSession
spark = SparkSession.builder.appName("Add Columns Example").getOrCreate()

# Create a Df
data = [("John", 25), ("Alice", 30), ("Bob", 35)]
df = spark.createDataFrame(data, ["Name", "Age"])

# Display the frame
print("Original DataFrame:")
df.show()

# Add a new column "City"
df_with_city = df.withColumn("City",lit("New York"))

from pyspark.sql.functions import when
df_with_gender = df_with_city.withColumn("Gender", when(df["Name"] == "John", "Male").otherwise("Female"))

# Display
print("DataFrame with added columns:")
df_with_gender.show()

spark.stop()
