from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, explode_outer, posexplode, posexplode_outer, lit


spark = SparkSession.builder.appName("Explode Functions").getOrCreate()

data = [
    (1, ["apple", "banana", "cherry"], None),
    (2, ["orange", "grape"], None),
    (3, None, None),
    (4, ["pen", "paper"], None),
    (5, None, {"A": 20, "B": 20, "C": 60}),
    (6, None, {"X": 80, "Y": 100})
]

df = spark.createDataFrame(data, ["id", "array_items", "map_items"])

exploded_array_df = df.select("id", explode_outer("array_items").alias("item"))
exploded_array_df.show()

pos_explode_map_df = df.select("id", posexplode_outer("map_items").alias("pos", "item"))
pos_explode_map_df.show()

spark.stop()
