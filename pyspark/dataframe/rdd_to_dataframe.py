from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD to Dataframe").getOrCreate()
sc=spark.sparkContext
data=[("Sathya",24),("Ravi",45),("priya",17)]

rdd=sc.parallelize(data)
print(rdd)
df=rdd.toDF()
df.printSchema()
df.show()

df1=spark.createDataFrame(rdd,["Name","Age"])
df1.printSchema()
df1.show()