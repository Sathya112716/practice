from pyspark.sql import SparkSession
from pyspark.sql.functions import explode_outer, posexplode_outer,explode,posexplode

spark = SparkSession.builder.appName("Explode Functions").getOrCreate()

data = [
    (1, ["apple", "banana", "cherry"]),
    (2, ["orange", "grape"]),
    (3, None),
    (4, ["pen", "paper"]),
]

df = spark.createDataFrame(data, ["id", "array_items"])


exploded_array_df = df.select("id", explode("array_items").alias("modified_items"))
exploded_array_df.show()

exploded_array_df = df.select("id", explode_outer("array_items").alias("modified_items"))
exploded_array_df.show()


pos_explode_map_df = df.select("id", posexplode("array_items").alias("pos", "item"))
pos_explode_map_df.show()

pos_explode_map_df = df.select("id", posexplode_outer("array_items").alias("pos", "item"))
pos_explode_map_df.show()

spark.stop()
