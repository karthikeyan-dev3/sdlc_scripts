import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, trim, upper, lower, coalesce, when, date_format
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Source paths
source_path = "s3://sdlc-agent-bucket/engineering-agent/bronze/"

# Target path
target_path = "s3://sdlc-agent-bucket/engineering-agent/silver/"

# Read sales_bronze
sales_df = spark.read.format("csv").option("header", "true").load(f"{source_path}/sales_bronze.csv/")
sales_df.createOrReplaceTempView("sales_bronze")

# Transform sales_silver
sales_query = """
SELECT 
    sb.transaction_id,
    sb.store_id,
    sb.product_id,
    sb.quantity,
    sb.sale_amount,
    CAST(sb.transaction_time AS DATE) AS sale_date
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id, store_id, product_id, transaction_time 
                              ORDER BY transaction_time DESC) AS rn
    FROM sales_bronze sb
    WHERE 
        sb.transaction_id IS NOT NULL 
        AND sb.store_id IS NOT NULL 
        AND sb.product_id IS NOT NULL
        AND sb.quantity > 0 
        AND sb.sale_amount > 0
) sb
WHERE sb.rn = 1
"""
sales_transformed = spark.sql(sales_query)
sales_transformed.write.mode("overwrite").csv(f"{target_path}/sales_silver.csv", header=True)

# Read products_bronze
products_df = spark.read.format("csv").option("header", "true").load(f"{source_path}/products_bronze.csv/")
products_df.createOrReplaceTempView("products_bronze")

# Transform products_silver
products_query = """
SELECT
    pb.product_id,
    TRIM(UPPER(pb.product_name)) AS product_name,
    pb.is_active
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY is_active DESC) AS rn
    FROM products_bronze pb
) pb
WHERE pb.rn = 1
"""
products_transformed = spark.sql(products_query)
products_transformed.write.mode("overwrite").csv(f"{target_path}/products_silver.csv", header=True)

# Read stores_bronze
stores_df = spark.read.format("csv").option("header", "true").load(f"{source_path}/stores_bronze.csv/")
stores_df.createOrReplaceTempView("stores_bronze")

# Transform stores_silver
stores_query = """
SELECT
    stb.store_id,
    TRIM(stb.store_name) AS store_name,
    TRIM(UPPER(stb.state)) AS state
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY CAST(open_date AS DATE) DESC) AS rn
    FROM stores_bronze stb
) stb
WHERE stb.rn = 1
"""
stores_transformed = spark.sql(stores_query)
stores_transformed.write.mode("overwrite").csv(f"{target_path}/stores_silver.csv", header=True)