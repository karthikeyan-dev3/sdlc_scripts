
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.sql.functions import col, trim, upper, concat, min as spark_min, current_timestamp
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Initialize GlueContext and SparkSession
glueContext = GlueContext(SparkSession.builder.getOrCreate())
spark = glueContext.spark_session

# Define source and target paths
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# Read and process sales_transactions_bronze
sales_transactions_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
sales_transactions_bronze_df.createOrReplaceTempView("stb")

sales_transactions_silver_df = spark.sql("""
    SELECT 
        transaction_id,
        UPPER(TRIM(store_id)) AS store_id,
        UPPER(TRIM(product_id)) AS product_id,
        CAST(transaction_time AS DATE) AS transaction_date,
        GREATEST(CAST(quantity AS INT), 0) AS quantity_sold,
        GREATEST(CAST(sale_amount AS DOUBLE), 0.0) AS revenue
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) as rn
        FROM stb
    ) WHERE rn = 1
""")

sales_transactions_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/sales_transactions_silver.csv", header=True)

# Read and process products_bronze
products_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
products_bronze_df.createOrReplaceTempView("pb")

products_silver_df = spark.sql("""
    SELECT
        UPPER(TRIM(product_id)) AS product_id,
        TRIM(product_name) AS product_name,
        LOWER(TRIM(category)) AS category
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY 
                               product_name DESC, category DESC, brand DESC, 
                               price DESC, is_active DESC) as rn
        FROM pb
    ) WHERE rn = 1 AND COALESCE(price, 0) >= 0
""")

products_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/products_silver.csv", header=True)

# Read and process stores_bronze
stores_bronze_df = spark.read.format(FILE_FORMAT).option("header", "true").load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
stores_bronze_df.createOrReplaceTempView("sb")

stores_silver_df = spark.sql("""
    SELECT
        UPPER(TRIM(store_id)) AS store_id,
        TRIM(store_name) AS store_name,
        CONCAT(TRIM(city), ', ', TRIM(state)) AS location
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY 
                               city DESC, state DESC, store_name DESC, 
                               open_date DESC) as rn
        FROM sb
    ) WHERE rn = 1
""")

stores_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/stores_silver.csv", header=True)

# Process product_store_mapping_silver from joined tables
sales_transactions_silver_df.createOrReplaceTempView("sts")
products_silver_df.createOrReplaceTempView("ps")
stores_silver_df.createOrReplaceTempView("ss")

product_store_mapping_silver_df = spark.sql("""
    SELECT 
        sts.product_id,
        sts.store_id,
        MIN(sts.transaction_date) AS availability_date
    FROM sts
    INNER JOIN ps ON sts.product_id = ps.product_id
    INNER JOIN ss ON sts.store_id = ss.store_id
    GROUP BY sts.product_id, sts.store_id
""")

product_store_mapping_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/product_store_mapping_silver.csv", header=True)

# Prepare data_quality_silver
data_quality_silver_df = spark.sql("""
    SELECT 
        stb.transaction_id AS record_id,
        CASE WHEN stb.transaction_id IS NULL THEN 'INVALID' ELSE 'VALID' END AS validation_status,
        CASE WHEN stb.transaction_id IS NULL THEN 'NULL_TRANSACTION_ID' ELSE 'VALID' END AS error_type,
        current_timestamp() AS last_updated
    FROM stb
""")

data_quality_silver_df.write.mode("overwrite").csv(f"{TARGET_PATH}/data_quality_silver.csv", header=True)
