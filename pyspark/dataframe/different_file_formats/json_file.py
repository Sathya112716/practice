from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

spark = SparkSession.builder.appName("Custom Schema JSON").getOrCreate()

# Define custom schema
custom_schema = StructType([
    StructField("name", StringType(), nullable=True),
    StructField("phoneNumber", StringType(), nullable=True),
    StructField("Email", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("userAgent", StringType(), nullable=True),
    StructField("hexcolor", StringType(), nullable=True)
])

# Read JSON file with custom schema
json_df = spark.read.schema(custom_schema).option("multiline","True").json("C:/Users/SathyaPriyaR/Desktop/pyspark/practice/pyspark/resources/sample 2 for practice.json")

# Show DataFrame
json_df.show()
json_df.printSchema()

spark.stop()


