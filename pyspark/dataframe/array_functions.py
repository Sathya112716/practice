from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark=SparkSession.builder.appName("Array Functions").getOrCreate()

data=[("Sathya",25),
      ("Ravi",30),
      ("Anu",18),
      ("Eniyan",30),
      ("Ambika",38)]
df=spark.createDataFrame(data,["name","age"])
array_df=df.withColumn("age_array",array("age"))
array_df.show()

#array_contains function
contains_df=array_df.withColumn("contains_30",array_contains("age_array",30))
contains_df.show()

#array_length function
length_df=array_df.withColumn("array_length",size("age_array"))
length_df.show()

#array_position function
position_df=array_df.withColumn("position_30",array_position("age_array",30))
position_df.show()

#array_remove function
remove_df=array_df.withColumn("age_array_filtered",array_remove("age_array",30))
remove_df.show()