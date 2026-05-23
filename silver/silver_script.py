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

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ----------------------------
# 1) SOURCE READS (S3 -> DF)
# ----------------------------
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

# ----------------------------
# 2) TEMP VIEWS
# ----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# TARGET: silver.product_master_silver
# ============================================================
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(pb.product_id) AS STRING)      AS product_id,
        CAST(TRIM(pb.product_name) AS STRING)    AS product_name,
        CAST(TRIM(pb.category) AS STRING)        AS product_category,
        CAST(pb.price AS DOUBLE)                 AS product_price
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL
        AND TRIM(pb.product_id) <> ''
        AND pb.is_active = 'true'
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        product_category,
        product_price,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY product_id) AS rn
      FROM base
    )
    SELECT
      CAST(product_id AS STRING)        AS product_id,
      CAST(product_name AS STRING)      AS product_name,
      CAST(product_category AS STRING)  AS product_category,
      CAST(product_price AS DOUBLE)     AS product_price
    FROM dedup
    WHERE rn = 1
    """
)

product_master_silver_output_df = product_master_silver_df.coalesce(1)

(
    product_master_silver_output_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

product_master_silver_df.createOrReplaceTempView("product_master_silver")

# ============================================================
# TARGET: silver.store_master_silver
# ============================================================
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(sb.store_id) AS STRING)                              AS store_id,
        CAST(TRIM(sb.store_name) AS STRING)                            AS store_name,
        CAST(CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS STRING)    AS store_location,
        CAST(TRIM(sb.state) AS STRING)                                 AS store_region
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL
        AND TRIM(sb.store_id) <> ''
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        store_location,
        store_region,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY store_id) AS rn
      FROM base
    )
    SELECT
      CAST(store_id AS STRING)        AS store_id,
      CAST(store_name AS STRING)      AS store_name,
      CAST(store_location AS STRING)  AS store_location,
      CAST(store_region AS STRING)    AS store_region
    FROM dedup
    WHERE rn = 1
    """
)

store_master_silver_output_df = store_master_silver_df.coalesce(1)

(
    store_master_silver_output_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

store_master_silver_df.createOrReplaceTempView("store_master_silver")

# ============================================================
# TARGET: silver.sales_transactions_silver
# ============================================================
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        CAST(TRIM(stb.transaction_id) AS STRING)          AS transaction_id,
        CAST(stb.transaction_time AS TIMESTAMP)          AS transaction_time,
        CAST(CAST(stb.transaction_time AS DATE) AS DATE) AS transaction_date,
        CAST(TRIM(stb.store_id) AS STRING)               AS store_id,
        CAST(TRIM(stb.product_id) AS STRING)             AS product_id,
        CAST(stb.quantity AS INT)                        AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE)                  AS total_sales_amount
      FROM sales_transactions_bronze stb
      INNER JOIN product_master_silver pms
        ON CAST(TRIM(stb.product_id) AS STRING) = CAST(pms.product_id AS STRING)
      INNER JOIN store_master_silver sms
        ON CAST(TRIM(stb.store_id) AS STRING) = CAST(sms.store_id AS STRING)
      WHERE TRIM(stb.transaction_id) IS NOT NULL
        AND TRIM(stb.transaction_id) <> ''
        AND TRIM(stb.store_id) IS NOT NULL
        AND TRIM(stb.store_id) <> ''
        AND TRIM(stb.product_id) IS NOT NULL
        AND TRIM(stb.product_id) <> ''
        AND CAST(stb.quantity AS INT) > 0
        AND CAST(stb.sale_amount AS DOUBLE) >= 0
    ),
    dedup AS (
      SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        total_sales_amount,
        ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY transaction_time DESC) AS rn
      FROM base
    )
    SELECT
      CAST(transaction_id AS STRING)      AS transaction_id,
      CAST(transaction_date AS DATE)      AS transaction_date,
      CAST(store_id AS STRING)            AS store_id,
      CAST(product_id AS STRING)          AS product_id,
      CAST(quantity_sold AS INT)          AS quantity_sold,
      CAST(total_sales_amount AS DOUBLE)  AS total_sales_amount
    FROM dedup
    WHERE rn = 1
    """
)

sales_transactions_silver_output_df = sales_transactions_silver_df.coalesce(1)

(
    sales_transactions_silver_output_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ============================================================
# TARGET: silver.aggregated_sales_silver
# ============================================================
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
      CAST(sts.store_id AS STRING)                   AS store_id,
      CAST(sts.product_id AS STRING)                 AS product_id,
      CAST(sts.transaction_date AS DATE)             AS sales_date,
      CAST(SUM(sts.quantity_sold) AS INT)            AS total_quantity_sold,
      CAST(SUM(sts.total_sales_amount) AS DOUBLE)    AS total_sales_amount
    FROM sales_transactions_silver sts
    GROUP BY
      CAST(sts.store_id AS STRING),
      CAST(sts.product_id AS STRING),
      CAST(sts.transaction_date AS DATE)
    """
)

aggregated_sales_silver_output_df = aggregated_sales_silver_df.coalesce(1)

(
    aggregated_sales_silver_output_df.write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()