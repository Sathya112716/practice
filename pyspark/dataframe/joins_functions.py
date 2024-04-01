from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName("Joins in Dataframe").getOrCreate()

emp= [(1,"Sathya", 24), (2,"Eniyan",16), (3,"Ambika", 50),(4,"Ravi",55),(5,"Priya",25)]
df1 = spark.createDataFrame(emp, ["emp_id","Name", "Age"])
print("DataFrame 1:",df1)
df1.show()

dept=[(1,"Marketing"),(2,"Finance"),(3,"Sales")]
df2=spark.createDataFrame(dept,["dept_id","dept_name"])
print("DataFrame 2:",df2)
df2.show()

df1.join(df2,df1.emp_id==df2.dept_id,"inner").show()

df1.join(df2,df1.emp_id==df2.dept_id,"left").show()

df1.join(df2,df1.emp_id==df2.dept_id,"right").show()

df1.join(df2,df1.emp_id==df2.dept_id,"outer").show()

df1.join(df2,df1.emp_id==df2.dept_id,"leftanti").show()

df1.join(df2,df1.emp_id==df2.dept_id,"leftsemi").show()
print("Broadcast Join:")
df1.join(broadcast(df2), df1.emp_id == df2.dept_id, "inner").show()

spark.stop()

spark.stop()#join function code