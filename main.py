from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DateType
from pyspark.sql.functions import month, year, quarter
from config import raw_csv_dir

spark = SparkSession.builder \
    .appName("SalesAnalysis") \
    .getOrCreate()

sales_schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("customer_id",StringType(),True),
    StructField("order_date",DateType(),True),
    StructField("location",StringType(),True),
    StructField("source_order",StringType(),True)
])

sales_df=spark.read.format("csv")\
    .option("header","false")\
    .option("inferschema", "true") \
    .schema(sales_schema)\
    .load(str(raw_csv_dir)+f"/sales.csv.txt")

# sales_df.show()

sales_df=sales_df.withColumn("order_year",year(sales_df.order_date))
sales_df=sales_df.withColumn("order_month",month(sales_df.order_date))
sales_df=sales_df.withColumn("order_qaurter",quarter(sales_df.order_date))

# sales_df.show()

menu_schema = StructType([
    StructField("menu_id",IntegerType(),True),
    StructField("menu_item",StringType(),True),
    StructField("price",StringType(),True)
])

menu_df=spark.read.format("csv")\
    .option("header","false")\
    .option("inferschema","true")\
    .schema(menu_schema)\
    .load(str(raw_csv_dir)+f"/menu.csv.txt")

# menu_df.show()