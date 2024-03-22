import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD Creation").getOrCreate()
sc=spark.sparkContext
data=[1,2,3,4,5,6,7,8,9,10,11,12]
rdd=sc.parallelize(data)
print("Action:First element:"+str(rdd.first()))#Action in default
#added the text file which is created in the local repo
rdd1=sc.textFile("jetbrains://pycharm/navigate/reference?project=pyspark&path=pyspark_practice/create a file")
#added the text file which created in the working directory"
rdd2=sc.textFile('C:\\Users\\SathyaPriyaR\\Desktop\\create_a_file.txt')
#create a empty rdd
rdd3=sc.emptyRDD()
rdd3=sc.parallelize([],10)
print("initial partition  count:"+str(rdd.getNumPartitions()))#transformation (transfering the data)
