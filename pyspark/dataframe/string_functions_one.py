from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("String Functions").getOrCreate()
sc = spark.sparkContext
data= [("   Sathya  ", 24, 1), ("  Ravi  ", 45, 2), ("  priya  ", 17, 3), ("  Anu  ", 18, 4), ("  Eniyan  ", 34, 5), ("  Sandhya ", 56, 6), (" Bhuvanesh ", 25, 7)]

rdd=sc.parallelize(data)
print(rdd)
df1=spark.createDataFrame(rdd,["Name","Age","Id"])
df1.printSchema()
#upper function
df1_upper = df1.withColumn("UPPER_NAME", upper(col("Name")))
df1_upper.printSchema()
df1_upper.show()

#lower function
df1_lower= df1.withColumn("lower_name", lower(col("Name")))
df1_lower.printSchema()
df1_lower.show()

#trim function
df1_trim= df1.withColumn("Trim_name", trim(col("Name")))
df1_trim.printSchema()
df1_trim.show() #Remove the whitespaces

#ltrim function
df1_ltrim= df1.withColumn("Left_Trimmed_name", ltrim(col("Name")))
df1_ltrim.printSchema()
df1_ltrim.show() #Remove the whitespace in the left side

#rtrim function
df1_rtrim=df1.withColumn("Right_Trimmed_name",rtrim(col("Name")))
df1_rtrim.printSchema()
df1_rtrim.show() #Remove the whitespace in the right side



