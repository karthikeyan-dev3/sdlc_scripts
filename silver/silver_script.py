import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ----------------------------
# Read Source Tables (Bronze)
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

sales_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_bronze.{FILE_FORMAT}/")
)

# ----------------------------
# Create Temp Views
# ----------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_bronze_df.createOrReplaceTempView("sales_bronze")

# ============================================================
# Target: silver.products_silver
# ============================================================
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        NULLIF(TRIM(pb.product_name), '') AS product_name,
        NULLIF(TRIM(pb.category), '') AS category,
        NULLIF(TRIM(pb.brand), '') AS brand,
        CASE
          WHEN pb.price IS NOT NULL AND CAST(pb.price AS DOUBLE) >= 0 THEN CAST(pb.price AS DOUBLE)
        END AS price,
        COALESCE(CAST(pb.is_active AS BOOLEAN), TRUE) AS is_active
      FROM products_bronze pb
      WHERE pb.product_id IS NOT NULL
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY is_active DESC, price DESC NULLS LAST, product_name ASC NULLS LAST
        ) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      price,
      is_active
    FROM dedup
    WHERE rn = 1
    """
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

products_silver_df.createOrReplaceTempView("sales_products_silver_tmp_view_for_downstream")
products_silver_df.createOrReplaceTempView("products_silver")

# ============================================================
# Target: silver.stores_silver
# ============================================================
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        NULLIF(TRIM(sb.store_name), '') AS store_name,
        NULLIF(TRIM(sb.city), '') AS city,
        NULLIF(TRIM(sb.state), '') AS state,
        NULLIF(TRIM(sb.store_type), '') AS store_type,
        CAST(sb.open_date AS DATE) AS open_date
      FROM stores_bronze sb
      WHERE sb.store_id IS NOT NULL
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY open_date DESC NULLS LAST, store_name ASC NULLS LAST
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      city,
      state,
      store_type,
      open_date
    FROM dedup
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

stores_silver_df.createOrReplaceTempView("stores_silver")

# ============================================================
# Target: silver.sales_silver
# ============================================================
sales_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(slb.transaction_id) AS sale_id,
        TRIM(slb.product_id) AS product_id,
        TRIM(slb.store_id) AS store_id,
        CAST(slb.transaction_time AS TIMESTAMP) AS transaction_time,
        CAST(slb.transaction_time AS DATE) AS sale_date,
        CASE
          WHEN slb.quantity IS NOT NULL AND CAST(slb.quantity AS BIGINT) >= 0 THEN CAST(slb.quantity AS BIGINT)
        END AS quantity_sold,
        CASE
          WHEN slb.sale_amount IS NOT NULL THEN CAST(slb.sale_amount AS DOUBLE)
        END AS sales_amount
      FROM sales_bronze slb
      WHERE slb.transaction_id IS NOT NULL
        AND slb.product_id IS NOT NULL
        AND slb.store_id IS NOT NULL
        AND slb.transaction_time IS NOT NULL
    ),
    dedup AS (
      SELECT
        *,
        ROW_NUMBER() OVER (
          PARTITION BY sale_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM base
    )
    SELECT
      sale_id,
      product_id,
      store_id,
      sale_date,
      quantity_sold,
      sales_amount,
      transaction_time
    FROM dedup
    WHERE rn = 1
    """
)

(
    sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_silver.csv")
)

sales_silver_df.createOrReplaceTempView("sales_silver")

# ============================================================
# Target: silver.sales_aggregated_silver
# ============================================================
sales_aggregated_silver_df = spark.sql(
    """
    SELECT
      ssal.sale_date AS date,
      SUM(COALESCE(ssal.sales_amount, 0)) AS total_sales_amount,
      SUM(COALESCE(ssal.quantity_sold, 0)) AS total_quantity_sold,
      AVG(ssal.sales_amount) AS average_sales_amount,
      COUNT(1) AS number_of_sales
    FROM sales_silver ssal
    GROUP BY ssal.sale_date
    """
)

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

job.commit()
