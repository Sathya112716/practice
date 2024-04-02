from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,col

spark = SparkSession.builder.appName("Unpivot Function").getOrCreate()

data = [("Product_A", 100, 150, 200),
        ("Product_B", 120, 180, 220),
        ("Product_C", 130, 160, 190)]

columns = ["Product", "Sales_Jan", "Sales_Feb", "Sales_Mar"]
df = spark.createDataFrame(data, columns)

df.show()

# Unpivot using stack
unpivot_df = df.select(
    col("Product"),
    expr("stack(3, 'Sales_Jan', Sales_Jan, 'Sales_Feb', Sales_Feb, 'Sales_Mar', Sales_Mar) as (Month, Sales)")
)

unpivot_df.show()
