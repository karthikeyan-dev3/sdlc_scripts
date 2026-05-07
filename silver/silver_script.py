import sys
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, monotonically_increasing_id
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, [])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver"
FILE_FORMAT = "csv"

# Store Master Silver
store_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
store_df.createOrReplaceTempView("stores_bronze")

store_master_silver = spark.sql("""
SELECT 
    sb.store_id,
    TRIM(UPPER(sb.store_name)) AS store_name,
    TRIM(UPPER(sb.state)) AS store_region,
    TRIM(UPPER(sb.city)) || ', ' || TRIM(UPPER(sb.state)) AS store_location
FROM 
    (SELECT *, row_number() OVER (PARTITION BY store_id ORDER BY store_id) as rn FROM stores_bronze) sb
WHERE 
    sb.rn = 1
""")
store_master_silver.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/store_master_silver.csv")

# Product Master Silver
product_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
product_df.createOrReplaceTempView("products_bronze")

product_master_silver = spark.sql("""
SELECT 
    pb.product_id,
    TRIM(UPPER(pb.product_name)) AS product_name,
    COALESCE(TRIM(UPPER(pb.category)), 'UNKNOWN') AS product_category
FROM 
    (SELECT *, row_number() OVER (PARTITION BY product_id ORDER BY product_id) as rn FROM products_bronze) pb
WHERE 
    pb.rn = 1
""")
product_master_silver.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/product_master_silver.csv")

# Sales Transactions Silver
sales_transactions_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
sales_transactions_df.createOrReplaceTempView("sales_transactions_bronze")

sales_transactions_silver = spark.sql("""
SELECT 
    stb.transaction_id,
    stb.store_id,
    stb.product_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    stb.quantity AS quantity_sold,
    stb.sale_amount AS total_revenue
FROM 
    (SELECT *, row_number() OVER (PARTITION BY transaction_id ORDER BY transaction_id) as rn FROM sales_transactions_bronze) stb
WHERE 
    stb.rn = 1
    AND stb.quantity >= 0
    AND stb.sale_amount >= 0
""")
sales_transactions_silver.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/sales_transactions_silver.csv")

# Silver Sales Summary
store_master_silver.createOrReplaceTempView("store_master_silver")
product_master_silver.createOrReplaceTempView("product_master_silver")
sales_transactions_silver.createOrReplaceTempView("sales_transactions_silver")

silver_sales_summary = spark.sql("""
SELECT 
    sts.transaction_id, 
    sts.store_id, 
    sts.product_id, 
    sts.transaction_date, 
    sts.total_revenue, 
    sts.quantity_sold,
    sms.store_location,
    sms.store_name, 
    sms.store_region,
    pms.product_name,
    pms.product_category
FROM 
    sales_transactions_silver sts
JOIN 
    store_master_silver sms 
ON 
    sts.store_id = sms.store_id
JOIN 
    product_master_silver pms 
ON 
    sts.product_id = pms.product_id
""")
silver_sales_summary.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/silver_sales_summary.csv")