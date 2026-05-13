from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, coalesce, trim, when, nullif
from pyspark.sql.window import Window
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName('AWS Glue PySpark Job') \
    .getOrCreate()

glueContext = GlueContext(spark.sparkContext)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read source data for sales_transactions
sales_transactions_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")

sales_transactions_df.createOrReplaceTempView("stb")

# Transform sales_transactions
sales_transactions_transformed = spark.sql("""
SELECT 
    stb.transaction_id,
    CAST(stb.transaction_time AS DATE) AS transaction_date,
    stb.store_id,
    stb.product_id,
    CASE WHEN COALESCE(stb.quantity, 0) < 0 THEN 0 ELSE COALESCE(stb.quantity, 0) END AS quantity_sold,
    CASE WHEN COALESCE(stb.sale_amount, 0) < 0 THEN 0 ELSE COALESCE(stb.sale_amount, 0) END AS sales_amount,
    CASE WHEN (CASE WHEN COALESCE(stb.quantity, 0) < 0 THEN 0 ELSE COALESCE(stb.quantity, 0) END) > 0
         THEN (CASE WHEN COALESCE(stb.sale_amount, 0) < 0 THEN 0 ELSE COALESCE(stb.sale_amount, 0) END) / 
              (CASE WHEN COALESCE(stb.quantity, 0) < 0 THEN 0 ELSE COALESCE(stb.quantity, 0) END)
         ELSE NULL END AS price_per_unit
FROM stb
WHERE stb.transaction_id IS NOT NULL
AND stb.store_id IS NOT NULL
AND stb.product_id IS NOT NULL
""")

sales_transactions_transformed.write.csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True, mode='overwrite')

# Read source data for products
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")

products_df.createOrReplaceTempView("pb")

# Transform products
products_transformed = spark.sql("""
SELECT 
    pb.product_id,
    NULLIF(TRIM(pb.product_name), '') AS product_name,
    NULLIF(TRIM(pb.category), '') AS category,
    NULLIF(TRIM(pb.brand), '') AS brand
FROM pb
WHERE pb.product_id IS NOT NULL
AND (pb.is_active = TRUE OR pb.is_active IS NULL)
""")

products_transformed.write.csv(f"{TARGET_PATH}/products_silver.csv", header=True, mode='overwrite')

# Read source data for stores
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")

stores_df.createOrReplaceTempView("sb")

# Transform stores
stores_transformed = spark.sql("""
SELECT 
    sb.store_id,
    NULLIF(TRIM(sb.store_name), '') AS store_name,
    COALESCE(NULLIF(TRIM(sb.state), ''), NULLIF(TRIM(sb.city), '')) AS store_region,
    NULL AS store_manager
FROM sb
WHERE sb.store_id IS NOT NULL
""")

stores_transformed.write.csv(f"{TARGET_PATH}/stores_silver.csv", header=True, mode='overwrite')