from pyspark.sql.types import StructType, StructField, IntegerType,StringType,DateType
from config import raw_csv_dir

schema = StructType([
    StructField("product_id",IntegerType(),True),
    StructField("customer_id",IntegerType(),True),
    StructField("order_date",IntegerType(),True),
    StructField("location",IntegerType(),True),
    StructField("source_order",IntegerType(),True),
])

