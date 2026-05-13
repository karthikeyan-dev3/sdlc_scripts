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
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

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

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save output: sales_transactions_silver
#    - Cast/standardize types, trim identifiers
#    - enforce positive quantity and non-negative sale_amount
#    - derive sale_date = CAST(transaction_time AS DATE)
#    - de-duplicate on transaction_id keeping latest transaction_time
# ------------------------------------------------------------------------------
sales_transactions_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(CAST(stb.transaction_id AS STRING))            AS transaction_id,
        CAST(stb.transaction_time AS TIMESTAMP)            AS transaction_time,
        CAST(CAST(stb.transaction_time AS TIMESTAMP) AS DATE) AS sale_date,
        TRIM(CAST(stb.store_id AS STRING))                 AS store_id,
        TRIM(CAST(stb.product_id AS STRING))               AS product_id,
        CAST(stb.quantity AS INT)                          AS quantity,
        CAST(stb.sale_amount AS DOUBLE)                    AS sale_amount,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(CAST(stb.transaction_id AS STRING))
          ORDER BY CAST(stb.transaction_time AS TIMESTAMP) DESC
        ) AS rn
      FROM sales_transactions_bronze stb
      WHERE
        CAST(stb.quantity AS INT) > 0
        AND CAST(stb.sale_amount AS DOUBLE) >= 0
    )
    SELECT
      transaction_id,
      transaction_time,
      sale_date,
      store_id,
      product_id,
      quantity,
      sale_amount
    FROM base
    WHERE rn = 1
    """
)

sales_transactions_silver_output = f"{TARGET_PATH}/sales_transactions_silver.csv"
(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(sales_transactions_silver_output)
)

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save output: products_silver
#    - Keep product_id, product_name, category, price
#    - standardize casing/whitespace, cast price to numeric
#    - filter active (is_active = true)
#    - de-duplicate on product_id keeping latest record (using product_id order only)
# ------------------------------------------------------------------------------
products_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(CAST(pb.product_id AS STRING))                 AS product_id,
        TRIM(CAST(pb.product_name AS STRING))              AS product_name,
        TRIM(CAST(pb.category AS STRING))                  AS category,
        CAST(pb.price AS DOUBLE)                           AS price,
        CAST(pb.is_active AS BOOLEAN)                      AS is_active,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(CAST(pb.product_id AS STRING))
          ORDER BY TRIM(CAST(pb.product_id AS STRING)) DESC
        ) AS rn
      FROM products_bronze pb
      WHERE CAST(pb.is_active AS BOOLEAN) = true
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

products_silver_output = f"{TARGET_PATH}/products_silver.csv"
(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(products_silver_output)
)

products_silver_df.createOrReplaceTempView("products_silver")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save output: stores_silver
#    - Keep store_id, store_name, location, region
#    - location = CONCAT(city, ', ', state)
#    - region = state (per UDT transformation)
#    - standardize casing/whitespace
#    - de-duplicate on store_id keeping latest record (using store_id order only)
# ------------------------------------------------------------------------------
stores_silver_df = spark.sql(
    """
    WITH base AS (
      SELECT
        TRIM(CAST(sb.store_id AS STRING))                   AS store_id,
        TRIM(CAST(sb.store_name AS STRING))                AS store_name,
        CONCAT(TRIM(CAST(sb.city AS STRING)), ', ', TRIM(CAST(sb.state AS STRING))) AS location,
        TRIM(CAST(sb.state AS STRING))                     AS region,
        ROW_NUMBER() OVER (
          PARTITION BY TRIM(CAST(sb.store_id AS STRING))
          ORDER BY TRIM(CAST(sb.store_id AS STRING)) DESC
        ) AS rn
      FROM stores_bronze sb
    )
    SELECT
      store_id,
      store_name,
      location,
      region
    FROM base
    WHERE rn = 1
    """
)

stores_silver_output = f"{TARGET_PATH}/stores_silver.csv"
(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(stores_silver_output)
)

stores_silver_df.createOrReplaceTempView("stores_silver")

# ------------------------------------------------------------------------------
# 3) Transform + 4) Save output: aggregated_sales_silver
#    - Daily store-product aggregation
#    - total_quantity_sold = SUM(quantity)
#    - total_sales_amount = SUM(sale_amount)
#    - group by store_id, product_id, sale_date
#    - exclude records with null keys
# ------------------------------------------------------------------------------
aggregated_sales_silver_df = spark.sql(
    """
    SELECT
      sts.store_id AS store_id,
      sts.product_id AS product_id,
      sts.sale_date AS sale_date,
      CAST(SUM(sts.quantity) AS INT) AS total_quantity_sold,
      CAST(SUM(sts.sale_amount) AS DOUBLE) AS total_sales_amount
    FROM sales_transactions_silver sts
    WHERE
      sts.store_id IS NOT NULL
      AND sts.product_id IS NOT NULL
      AND sts.sale_date IS NOT NULL
    GROUP BY
      sts.store_id,
      sts.product_id,
      sts.sale_date
    """
)

aggregated_sales_silver_output = f"{TARGET_PATH}/aggregated_sales_silver.csv"
(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(aggregated_sales_silver_output)
)

job.commit()