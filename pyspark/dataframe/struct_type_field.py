from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, IntegerType, StructField, StructType
spark = SparkSession.builder.appName("StructType_vs_StructField").getOrCreate()

name = StructField("Name", StringType(), nullable=True)
age= StructField("Age", IntegerType(), nullable=True)

person_schema = StructType([name, age])


data = [("Sathya", 24), ("Priya", 17), ("Ravi", 18)]

df = spark.createDataFrame(data, schema=person_schema)
for field in person_schema.fields:
    print(field.name)

print()
for col_name in df.columns:
    print(col_name)
print()
print("DataFrame Schema:")
df.printSchema()
df.show()
