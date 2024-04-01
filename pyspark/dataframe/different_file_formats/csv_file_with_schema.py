from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType

spark = SparkSession.builder.appName("CSV file formats").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Define schema for CSV file
csv_schema = StructType([
    StructField("index", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("company", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True),
    StructField("phone_1", StringType(), True),
    StructField("phone_2", StringType(), True),
    StructField("Email", StringType(), True),
    StructField("subscription_date", DateType(), True),
    StructField("website", StringType(), True)
])


# using inferschema
infer_schema_df = spark.read.csv(path=r'C:\Users\SathyaPriyaR\Desktop\pyspark\practice\pyspark\resources\customers.csv' , header=True , inferSchema=True)
infer_schema_df.show()
infer_schema_df.printSchema()


#using custom schema reading a csv file

custom_schema_df = spark.read.csv(path=r'C:\Users\SathyaPriyaR\Desktop\pyspark\practice\pyspark\resources\customers.csv' , schema=csv_schema , header=True)
custom_schema_df.show()
custom_schema_df.printSchema()