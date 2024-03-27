from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("String Functions").getOrCreate()
data= [("Sathya@Priya","Computer Science",1),("Ravi@Chandran","Business",2),("Bhuvaneshwar@Karthick","Electronics",3)]

rdd = spark.sparkContext.parallelize(data)
df = spark.createDataFrame(rdd,["Name","Department","Id"])

df.printSchema()
df.show()

# Using regexp_extract to extract the first word from the "Name" column
df_extract = df.withColumn("First_Word", regexp_extract(df["Name"],"@%^#" , 1))
df_extract.show()
