from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("Pivot Function").getOrCreate()

data=[("Sathya","Physics",70),("Negha","Chemistry",80),("Ravi","Maths",75)]
df=spark.createDataFrame(data,["name","subject","score"])

pivot_df=df.groupBy("name").pivot("subject").avg("score")
pivot_df.show()