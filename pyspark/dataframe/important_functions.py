from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, expr

spark = SparkSession.builder.appName("Functions Example").getOrCreate()

data = [
    ("Ravi", "male", 50),
    ("Sathya", "female", 25),
    ("Eniyan", "male", 15)
]

df = spark.createDataFrame(data, ["name", "gender", "age"])

def string_length(name):
    return len(name)
df = df.withColumn("name_length", string_length(df["name"]))

# Pivot example
pivoted_df = df.groupBy("gender").pivot("name").sum("age")

# Calculate counts for each gender
gender_counts_df = df.groupBy("gender").count()

# Unpivot example using stack()
unpivoted_df = gender_counts_df.selectExpr("gender", "stack(2, 'male', male_count, 'female', female_count) as (gender, count)")

# Transform example
transformed_df = df.withColumn("doubled_age", expr("transform(age, x -> x * 2)"))

print("DataFrame with UDF applied:")
df.show()

print("Pivoted DataFrame:")
pivoted_df.show()

print("Unpivoted DataFrame:")
unpivoted_df.show()

print("Transformed DataFrame:")
transformed_df.show()

spark.stop()
