
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------------------

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

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------

products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ------------------------------------------------------------------------------
# Target: product_master_silver
# ------------------------------------------------------------------------------

product_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(pb.product_id) AS STRING) AS product_id,
            CAST(TRIM(pb.product_name) AS STRING) AS product_name,
            CAST(TRIM(pb.category) AS STRING) AS category,
            CAST(TRIM(pb.brand) AS STRING) AS brand,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(pb.product_id)
                ORDER BY TRIM(pb.product_id) DESC
            ) AS rn
        FROM products_bronze pb
        WHERE pb.product_id IS NOT NULL
          AND TRIM(pb.product_id) <> ''
    )
    SELECT
        product_id,
        product_name,
        category,
        brand
    FROM base
    WHERE rn = 1
    """
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: store_master_silver
# ------------------------------------------------------------------------------

store_master_silver_df = spark.sql(
    """
    WITH base AS (
        SELECT
            CAST(TRIM(sb.store_id) AS STRING) AS store_id,
            CAST(TRIM(sb.store_name) AS STRING) AS store_name,
            CAST(CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS STRING) AS location,
            CAST(TRIM(sb.store_type) AS STRING) AS store_type,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(sb.store_id)
                ORDER BY TRIM(sb.store_id) DESC
            ) AS rn
        FROM stores_bronze sb
        WHERE sb.store_id IS NOT NULL
          AND TRIM(sb.store_id) <> ''
    )
    SELECT
        store_id,
        store_name,
        location,
        store_type
    FROM base
    WHERE rn = 1
    """
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ------------------------------------------------------------------------------
# Target: sales_transactions_silver
# ------------------------------------------------------------------------------

sales_transactions_silver_df = spark.sql(
    """
    WITH dedup AS (
        SELECT
            CAST(TRIM(stb.transaction_id) AS STRING) AS transaction_id,
            CAST(stb.transaction_time AS TIMESTAMP) AS transaction_time,
            CAST(stb.transaction_time AS DATE) AS sale_date,
            CAST(TRIM(stb.product_id) AS STRING) AS product_id,
            CAST(TRIM(stb.store_id) AS STRING) AS store_id,
            CAST(stb.quantity AS INT) AS quantity_sold,
            CAST(stb.sale_amount AS DOUBLE) AS total_sales_amount,
            ROW_NUMBER() OVER (
                PARTITION BY TRIM(stb.transaction_id)
                ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
            ) AS rn
        FROM sales_transactions_bronze stb
        WHERE stb.transaction_id IS NOT NULL
          AND TRIM(stb.transaction_id) <> ''
    ),
    cleaned AS (
        SELECT
            d.transaction_id,
            d.sale_date,
            d.product_id,
            d.store_id,
            CASE WHEN d.quantity_sold < 0 THEN NULL ELSE d.quantity_sold END AS quantity_sold,
            CASE WHEN d.total_sales_amount < 0 THEN NULL ELSE d.total_sales_amount END AS total_sales_amount
        FROM dedup d
        WHERE d.rn = 1
    )
    SELECT
        c.transaction_id,
        c.sale_date,
        c.product_id,
        c.store_id,
        c.quantity_sold,
        c.total_sales_amount
    FROM cleaned c
    LEFT JOIN product_master_silver pms
        ON c.product_id = pms.product_id
    LEFT JOIN store_master_silver sms
        ON c.store_id = sms.store_id
    WHERE pms.product_id IS NOT NULL
      AND sms.store_id IS NOT NULL
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
