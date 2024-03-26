from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Broadcast Variable").getOrCreate()
sc = spark.sparkContext

empDF = [("Sathya", 24, 1), ("Ravi", 45, 2), ("priya", 17, 3), ("Anu", 18, 4), ("Eniyan", 34, 5), ("Sandhya", 56, 6), ("Bhuvanesh", 25, 7)]

DeptDF = [("Finance", 1), ("Sales", 2), ("Marketing", 3), ("Computer Science", 4), ("Data Engineer", 5), ("Electronics", 6), ("Arts", 7)]
rdd_emp = sc.parallelize(empDF)
rdd_dept = sc.parallelize(DeptDF)

df_emp = spark.createDataFrame(rdd_emp, ["Name", "Age", "EmployeeId"])
df_dept = spark.createDataFrame(rdd_dept, ["DepartmentName", "DeptId"])
broadcast_dept = sc.broadcast(DeptDF)

joinedDF = df_emp.join(df_dept, df_emp.EmployeeId == df_dept.DeptId, 'inner')

joinedDF.explain()
joinedDF.show()
