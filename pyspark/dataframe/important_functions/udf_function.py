from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("UDF Functions").getOrCreate()

data = [
    ("Ravi", "male", 50),
    ("Sathya", "female", 25),
    ("Eniyan", "male", 15)
]

df = spark.createDataFrame(data, ["name", "gender", "age"])

# Define UDF
@udf(returnType=IntegerType())
def string_length(name):
    return len(name)

# Apply UDF
df = df.withColumn("name_length", string_length(df["name"]))
df.show()
