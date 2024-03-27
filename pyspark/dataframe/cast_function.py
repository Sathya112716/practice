from pyspark.sql.functions import col
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Cast Function").getOrCreate()
sc = spark.sparkContext

data= [("Sathya", "24", 1), ("Ravi", "45", 2), ("priya", "17", 3), ("Anu", "18", 4), ("Eniyan", "34", 5), ("Sandhya", "56", 6), ("Bhuvanesh", "25", 7)]

rdd=sc.parallelize(data)
print(rdd)
df1=spark.createDataFrame(rdd,["Name","Age","Id"])
print("Before Casting:")
df1.printSchema()
spark.stop()

df1_casted=df1.withColumn("Age",col("Age").cast("int"))
print("After casting:")
df1_casted.printSchema()