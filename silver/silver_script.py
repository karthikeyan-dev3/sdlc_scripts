from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, trim, upper, lower, date_format, coalesce
from pyspark.sql.window import Window

# Initialize Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = SparkSession.builder.appName("AWS Glue PySpark Job").getOrCreate()

# Define Source and Target Paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Load Sales Data
sales_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
sales_df.createOrReplaceTempView("sales_bronze")

# Transform and Write Sales Data
sales_silver_query = """
SELECT
    sb.transaction_id,
    sb.product_id,
    sb.store_id,
    DATE(sb.transaction_time) AS sale_date,
    sb.quantity AS quantity_sold,
    sb.sale_amount AS total_sales_amount
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
    FROM sales_bronze
    WHERE transaction_id IS NOT NULL
      AND store_id IS NOT NULL
      AND product_id IS NOT NULL
      AND quantity > 0
      AND sale_amount >= 0
) sb
WHERE sb.rn = 1
"""
sales_silver_df = spark.sql(sales_silver_query)
sales_silver_df.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/sales_silver.csv")

# Load Products Data
products_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
products_df.createOrReplaceTempView("products_bronze")

# Transform and Write Products Data
products_silver_query = """
SELECT
    pb.product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    pb.brand AS manufacturer
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY is_active DESC) AS rn
    FROM products_bronze
    WHERE is_active = true
) pb
WHERE pb.rn = 1
"""
products_silver_df = spark.sql(products_silver_query)
products_silver_df.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/products_silver.csv")

# Load Stores Data
stores_df = spark.read \
    .format(FILE_FORMAT) \
    .option("header", "true") \
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
stores_df.createOrReplaceTempView("stores_bronze")

# Transform and Write Stores Data
stores_silver_query = """
SELECT
    stb.store_id,
    TRIM(stb.store_name) AS store_name,
    CONCAT(TRIM(stb.city), ', ', TRIM(stb.state)) AS location,
    TRIM(stb.state) AS region
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_name) AS rn
    FROM stores_bronze
) stb
WHERE stb.rn = 1
"""
stores_silver_df = spark.sql(stores_silver_query)
stores_silver_df.write.mode("overwrite").option("header", "true").csv(f"{TARGET_PATH}/stores_silver.csv")