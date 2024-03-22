import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD Actions").getOrCreate()

# Parallelize a list to create a new RDD
data = [1, 2, 3, 4, 5]
rdd = spark.sparkContext.parallelize(data) #create a rddd

collected_data = rdd.collect()
print("Collected data:", collected_data)

count = rdd.count()
print("Count:", count)

first_element = rdd.first()
print("First element:", first_element)


taken_elements = rdd.take(3)
print("Taken elements:", taken_elements)

ordered_elements = rdd.takeOrdered(3)
print("Ordered elements:", ordered_elements)

sum_of_elements = rdd.reduce(lambda x, y: x + y)
print("Sum of elements:", sum_of_elements)

min_element = rdd.min()
print("Minimum element:", min_element)

max_element = rdd.max()
print("Maximum element:", max_element)
spark.stop()
