import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
 
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
 
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"
 
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
 
# =============================================================================
# 1) Read source tables
# =============================================================================
products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
 
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
 
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)
 
# =============================================================================
# 2) Create temp views
# =============================================================================
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
 
# =============================================================================
# 3) Transform + 4) Save output: products_silver
# =============================================================================
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        CAST(pb.price AS FLOAT) AS price,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(pb.product_id)
          ORDER BY
            CASE WHEN TRIM(pb.product_name) IS NOT NULL AND TRIM(pb.product_name) <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN TRIM(pb.category) IS NOT NULL AND TRIM(pb.category) <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN pb.price IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL
        AND TRIM(pb.product_id) <> ''
        AND (CAST(pb.price AS FLOAT) IS NULL OR CAST(pb.price AS FLOAT) >= 0)
    )
    SELECT
      product_id,
      product_name,
      category,
      price
    FROM base
    WHERE rn = 1
    """
)
products_silver_df.createOrReplaceTempView("products_silver")
 
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)
 
# =============================================================================
# 3) Transform + 4) Save output: stores_silver
# =============================================================================
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        TRIM(sb.city) AS city,
        TRIM(sb.state) AS state,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(sb.store_id)
          ORDER BY
            CASE WHEN TRIM(sb.store_name) IS NOT NULL AND TRIM(sb.store_name) <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN TRIM(sb.city) IS NOT NULL AND TRIM(sb.city) <> '' THEN 1 ELSE 0 END DESC,
            CASE WHEN TRIM(sb.state) IS NOT NULL AND TRIM(sb.state) <> '' THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL
        AND TRIM(sb.store_id) <> ''
    )
    SELECT
      store_id,
      store_name,
      CONCAT(city, ', ', state) AS location,
      CAST(NULL AS STRING) AS manager
    FROM base
    WHERE rn = 1
    """
)
 
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)
 
# =============================================================================
# 3) Transform + 4) Save output: sales_transactions_silver
# =============================================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        TRIM(stb.product_id) AS product_id,
        TRIM(stb.store_id) AS store_id,
        CAST(stb.quantity AS INT) AS quantity,
        CAST(COALESCE(CAST(stb.sale_amount AS DOUBLE), CAST(stb.quantity AS DOUBLE) * CAST(ps.price AS DOUBLE)) AS DOUBLE) AS revenue,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(stb.transaction_id)
          ORDER BY
            CASE WHEN stb.transaction_time IS NOT NULL THEN 1 ELSE 0 END DESC,
            CASE WHEN stb.sale_amount IS NOT NULL OR (stb.quantity IS NOT NULL AND ps.price IS NOT NULL) THEN 1 ELSE 0 END DESC,
            CASE WHEN stb.quantity IS NOT NULL THEN 1 ELSE 0 END DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      LEFT JOIN products_silver ps
        ON TRIM(stb.product_id) = ps.product_id
      WHERE TRIM(stb.transaction_id) IS NOT NULL
        AND TRIM(stb.transaction_id) <> ''
        AND CAST(stb.quantity AS INT) > 0
    )
    SELECT
      transaction_id,
      product_id,
      store_id,
      quantity,
      revenue,
      transaction_date
    FROM base
    WHERE rn = 1
    """
)
 
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)
 
job.commit()