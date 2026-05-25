import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
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

# -------------------------------------------------------------------
# 1) Read source tables from S3
# -------------------------------------------------------------------
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

# -------------------------------------------------------------------
# 2) Create temp views
# -------------------------------------------------------------------
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# -------------------------------------------------------------------
# Target: silver.product_master_silver
# -------------------------------------------------------------------
product_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(pb.product_id) AS product_id,
        TRIM(pb.product_name) AS product_name,
        TRIM(pb.category) AS category,
        TRIM(pb.brand) AS brand,
        CAST(pb.price AS DOUBLE) AS unit_price
      FROM products_bronze pb
      WHERE TRIM(pb.product_id) IS NOT NULL AND TRIM(pb.product_id) <> ''
    ),
    dedup AS (
      SELECT
        product_id,
        product_name,
        category,
        brand,
        unit_price,
        ROW_NUMBER() OVER (
          PARTITION BY product_id
          ORDER BY product_id
        ) AS rn
      FROM base
    )
    SELECT
      product_id,
      product_name,
      category,
      brand,
      unit_price
    FROM dedup
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

# -------------------------------------------------------------------
# Target: silver.store_master_silver
# -------------------------------------------------------------------
store_master_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(sb.store_id) AS store_id,
        TRIM(sb.store_name) AS store_name,
        CONCAT(TRIM(sb.city), ', ', TRIM(sb.state)) AS location,
        TRIM(sb.state) AS region
      FROM stores_bronze sb
      WHERE TRIM(sb.store_id) IS NOT NULL AND TRIM(sb.store_id) <> ''
    ),
    dedup AS (
      SELECT
        store_id,
        store_name,
        location,
        region,
        ROW_NUMBER() OVER (
          PARTITION BY store_id
          ORDER BY store_id
        ) AS rn
      FROM base
    )
    SELECT
      store_id,
      store_name,
      location,
      region
    FROM dedup
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

# -------------------------------------------------------------------
# Target: silver.sales_transactions_silver
# -------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(stb.transaction_id) AS transaction_id,
        CAST(stb.transaction_time AS DATE) AS transaction_date,
        TRIM(stb.store_id) AS store_id,
        TRIM(stb.product_id) AS product_id,
        CAST(stb.quantity AS INT) AS quantity_sold,
        CAST(stb.sale_amount AS DOUBLE) AS sales_amount,
        CAST(GREATEST((pms.unit_price * CAST(stb.quantity AS INT)) - CAST(stb.sale_amount AS DOUBLE), 0) AS DOUBLE) AS discount_amount,
        stb.transaction_time AS transaction_time
      FROM sales_transactions_bronze stb
      LEFT JOIN product_master_silver pms
        ON TRIM(stb.product_id) = pms.product_id
      LEFT JOIN store_master_silver sms
        ON TRIM(stb.store_id) = sms.store_id
      WHERE TRIM(stb.transaction_id) IS NOT NULL AND TRIM(stb.transaction_id) <> ''
    ),
    dedup AS (
      SELECT
        transaction_id,
        transaction_date,
        store_id,
        product_id,
        quantity_sold,
        sales_amount,
        discount_amount,
        ROW_NUMBER() OVER (
          PARTITION BY transaction_id
          ORDER BY transaction_time DESC
        ) AS rn
      FROM base
    )
    SELECT
      transaction_id,
      transaction_date,
      store_id,
      product_id,
      quantity_sold,
      sales_amount,
      discount_amount
    FROM dedup
    WHERE rn = 1
    """
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# -------------------------------------------------------------------
# Target: silver.aggregated_sales_silver
# -------------------------------------------------------------------
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
      sts.transaction_date AS date,
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      CAST(SUM(sts.quantity_sold) AS INT) AS total_quantity_sold,
      CAST(SUM(sts.sales_amount) AS DOUBLE) AS total_sales_amount,
      CAST(SUM(sts.discount_amount) AS DOUBLE) AS total_discount_amount,
      CAST(SUM(sts.sales_amount) / SUM(sts.quantity_sold) AS DOUBLE) AS average_unit_price
    FROM sales_transactions_silver sts
    GROUP BY
      sts.transaction_date,
      sts.store_id,
      sts.product_id
    """
)

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

job.commit()