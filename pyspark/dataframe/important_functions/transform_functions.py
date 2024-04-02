from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark=SparkSession.builder.appName("Transform function").getOrCreate()
data=[("Sathya",30),("Priya",25),("Eniyan",25)]
df=spark.createDataFrame(data,["name","age"])

#transform function
transform_df=df.withColumn("age_in_months",col("age")*12)
transform_df.show()