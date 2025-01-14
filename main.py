from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DateType
from pyspark.sql.functions import month, year, quarter
from config import raw_csv_dir

spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .getOrCreate()

schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True)
])

sales_df=spark.read.format("csv")\
    .option("header","true")\
    .option("inferschema", "true") \
    .schema(schema)\
    .load(str(raw_csv_dir)+f"/sales.csv.txt")

sales_df.show()

sales_df=sales_df.withColumn("order_year",year(sales_df.order_date))
sales_df=sales_df.withColumn("order_month",month(sales_df.order_date))
sales_df=sales_df.withColumn("order_qaurter",quarter(sales_df.order_date))

sales_df.show()