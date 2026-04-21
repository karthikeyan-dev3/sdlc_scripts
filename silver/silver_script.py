
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

# ------------------------------------------------------------------------------------
# AWS Glue Setup
# ------------------------------------------------------------------------------------
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glue_context = GlueContext(sc)
spark: SparkSession = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# Config
# ------------------------------------------------------------------------------------
SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Source Reads (Bronze) + Temp Views
# NOTE: Path format is строго per requirement: .load(f"{SOURCE_PATH}/table_name.{FILE_FORMAT}/")
# ------------------------------------------------------------------------------------
stores_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/stores_bronze.{FILE_FORMAT}/")
)
stores_bronze_df.createOrReplaceTempView("stores_bronze")

products_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/products_bronze.{FILE_FORMAT}/")
)
products_bronze_df.createOrReplaceTempView("products_bronze")

transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/transactions_bronze.{FILE_FORMAT}/")
)
transactions_bronze_df.createOrReplaceTempView("transactions_bronze")

# ====================================================================================
# TARGET TABLE: silver.dim_stores_silver
# Mapping: bronze.stores_bronze sb QUALIFY ROW_NUMBER() OVER (PARTITION BY sb.store_id
#          ORDER BY sb.open_date DESC, sb.store_name DESC) = 1
# ====================================================================================
dim_stores_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        -- Exact column lineage (ds.* = sb.*) with standardization using allowed functions
        CAST(TRIM(sb.store_id) AS STRING)                                  AS store_id,
        CAST(TRIM(sb.store_name) AS STRING)                                AS store_name,
        CAST(UPPER(TRIM(sb.city)) AS STRING)                               AS city,
        CAST(LOWER(TRIM(sb.store_type)) AS STRING)                         AS store_type,
        ROW_NUMBER() OVER (
          PARTITION BY sb.store_id
          ORDER BY sb.open_date DESC, sb.store_name DESC
        ) AS rn
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL AND TRIM(sb.store_id) <> ''
    )
    SELECT
      store_id,
      store_name,
      city,
      store_type
    FROM ranked
    WHERE rn = 1
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    dim_stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_stores_silver.csv")
)

# Create temp view for downstream joins
dim_stores_silver_df.createOrReplaceTempView("dim_stores_silver")

# ====================================================================================
# TARGET TABLE: silver.dim_products_silver
# Mapping: bronze.products_bronze pb QUALIFY ROW_NUMBER() OVER (PARTITION BY pb.product_id
#          ORDER BY pb.is_active DESC, pb.product_name DESC) = 1
# ====================================================================================
dim_products_silver_df = spark.sql(
    """
    WITH ranked AS (
      SELECT
        -- Exact column lineage (dp.* = pb.*) with standardization using allowed functions
        CAST(TRIM(pb.product_id) AS STRING)                                AS product_id,
        CAST(TRIM(pb.product_name) AS STRING)                              AS product_name,
        CAST(LOWER(TRIM(pb.category)) AS STRING)                           AS category,
        CAST(TRIM(pb.brand) AS STRING)                                     AS brand,
        ROW_NUMBER() OVER (
          PARTITION BY pb.product_id
          ORDER BY pb.is_active DESC, pb.product_name DESC
        ) AS rn
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL AND TRIM(pb.product_id) <> ''
    )
    SELECT
      product_id,
      product_name,
      category,
      brand
    FROM ranked
    WHERE rn = 1
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    dim_products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/dim_products_silver.csv")
)

# Create temp view for downstream joins
dim_products_silver_df.createOrReplaceTempView("dim_products_silver")

# ====================================================================================
# TARGET TABLE: silver.fact_transactions_silver
# Mapping:
# bronze.transactions_bronze tb
#   INNER JOIN silver.dim_stores_silver ds ON tb.store_id = ds.store_id
#   INNER JOIN silver.dim_products_silver dp ON tb.product_id = dp.product_id
# Transform: sales_date = CAST(tb.transaction_time AS DATE)
# Filters: non-null keys, quantity > 0, sale_amount >= 0
# ====================================================================================
fact_transactions_silver_df = spark.sql(
    """
    SELECT
      -- Exact column lineage per UDT (ft.* = tb.*) + derived sales_date
      CAST(TRIM(tb.transaction_id) AS STRING)                              AS transaction_id,
      CAST(TRIM(tb.store_id) AS STRING)                                    AS store_id,
      CAST(TRIM(tb.product_id) AS STRING)                                  AS product_id,
      CAST(tb.transaction_time AS TIMESTAMP)                               AS transaction_time,
      CAST(tb.quantity AS INT)                                             AS quantity,
      CAST(tb.sale_amount AS DECIMAL(38, 10))                              AS sale_amount,
      CAST(tb.transaction_time AS DATE)                                    AS sales_date
    FROM transactions_bronze tb
    INNER JOIN dim_stores_silver ds
      ON CAST(TRIM(tb.store_id) AS STRING) = ds.store_id
    INNER JOIN dim_products_silver dp
      ON CAST(TRIM(tb.product_id) AS STRING) = dp.product_id
    WHERE
      TRIM(tb.transaction_id) IS NOT NULL AND TRIM(tb.transaction_id) <> ''
      AND TRIM(tb.store_id) IS NOT NULL AND TRIM(tb.store_id) <> ''
      AND TRIM(tb.product_id) IS NOT NULL AND TRIM(tb.product_id) <> ''
      AND CAST(tb.quantity AS INT) > 0
      AND CAST(tb.sale_amount AS DECIMAL(38, 10)) >= 0
    """
)

# Write as SINGLE CSV file directly under TARGET_PATH (no subfolders)
(
    fact_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/fact_transactions_silver.csv")
)

job.commit()
