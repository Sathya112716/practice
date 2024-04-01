from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("UnionExample").getOrCreate()

df1 = spark.createDataFrame([(1, "Sathya"), (2, "Priya"), (3, "Ravi")], ["id", "name"])
df2 = spark.createDataFrame([(3, "Sandhya"), (4, "Eniyan"), (5, "Sathya")], ["id", "name"])

union_df = df1.union(df2)
print("Union using union():")
union_df.show()


union_all_df = df1.unionAll(df2)
print("Union using unionAll():")
union_all_df.show()

df3 = spark.createDataFrame([(1, "Anu"), (2, "Lalitha"), (3, "Sathya")], ["id", "name"])


union_by_name_df = df1.unionByName(df3)
print("Union using unionByName():")
union_by_name_df.show()

spark.stop()
