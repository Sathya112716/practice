from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,ArrayType

spark = SparkSession.builder.appName("Json file").getOrCreate()

# Define custom schema
custom_schema = StructType([
    StructField("countries" , ArrayType(
        StructType([
    StructField("name", StringType(), nullable=True),
    StructField("code", StringType(), nullable=True),
    StructField("rank", IntegerType(), nullable=True)
])), nullable=True)
])


# Read JSON file with custom schema
json_df = spark.read.schema(custom_schema).option("multiline","True").json("C:\\Users\\SathyaPriyaR\\Desktop\\pyspark\\practice\\pyspark\\resources\\sample json for practice.json")

# Show DataFrame
json_df.show()
json_df.printSchema()

spark.stop()
