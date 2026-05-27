import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/bronze/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

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
# 2) Create temp views (bronze schema namespace)
# ------------------------------------------------------------------------------

products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")
spark.sql("CREATE DATABASE IF NOT EXISTS silver")

spark.sql("CREATE OR REPLACE TEMP VIEW bronze.products_bronze AS SELECT * FROM products_bronze")
spark.sql("CREATE OR REPLACE TEMP VIEW bronze.stores_bronze AS SELECT * FROM stores_bronze")
spark.sql(
    "CREATE OR REPLACE TEMP VIEW bronze.sales_transactions_bronze AS SELECT * FROM sales_transactions_bronze"
)

# ------------------------------------------------------------------------------
# TARGET: silver.products_silver
# ------------------------------------------------------------------------------

products_silver_df = spark.sql(
    """
SELECT
  product_id,
  TRIM(product_name) AS product_name,
  TRIM(category) AS category,
  TRIM(brand) AS brand,
  CAST(price AS DOUBLE) AS price,
  COALESCE(is_active, TRUE) AS is_active
FROM (
  SELECT
    pb.product_id,
    pb.product_name,
    pb.category,
    pb.brand,
    pb.price,
    pb.is_active,
    ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_id) AS rn
  FROM bronze.products_bronze pb
  WHERE pb.product_id IS NOT NULL
) d
WHERE d.rn = 1
  AND d.is_active = TRUE
"""
)

(
    products_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/products_silver.csv")
)

# ------------------------------------------------------------------------------
# TARGET: silver.stores_silver
# ------------------------------------------------------------------------------

stores_silver_df = spark.sql(
    """
SELECT
  store_id,
  TRIM(store_name) AS store_name,
  TRIM(city) AS city,
  TRIM(state) AS state,
  TRIM(store_type) AS store_type,
  CAST(open_date AS DATE) AS open_date
FROM (
  SELECT
    sb.store_id,
    sb.store_name,
    sb.city,
    sb.state,
    sb.store_type,
    sb.open_date,
    ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.store_id) AS rn
  FROM bronze.stores_bronze sb
  WHERE sb.store_id IS NOT NULL
) d
WHERE d.rn = 1
"""
)

(
    stores_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/stores_silver.csv")
)

# ------------------------------------------------------------------------------
# TARGET: silver.sales_transactions_silver
# ------------------------------------------------------------------------------

sales_transactions_silver_df = spark.sql(
    """
SELECT
  transaction_id,
  TRIM(store_id) AS store_id,
  TRIM(product_id) AS product_id,
  CAST(quantity AS INT) AS quantity,
  CAST(sale_amount AS DOUBLE) AS sale_amount,
  CAST(transaction_time AS TIMESTAMP) AS transaction_time,
  CAST(transaction_time AS DATE) AS sales_date
FROM (
  SELECT
    stb.transaction_id,
    stb.store_id,
    stb.product_id,
    stb.quantity,
    stb.sale_amount,
    stb.transaction_time,
    ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
  FROM bronze.sales_transactions_bronze stb
  WHERE stb.transaction_id IS NOT NULL
) d
WHERE d.rn = 1
  AND d.transaction_time IS NOT NULL
  AND d.store_id IS NOT NULL
  AND d.product_id IS NOT NULL
  AND d.quantity IS NOT NULL
  AND d.quantity > 0
  AND d.sale_amount IS NOT NULL
  AND d.sale_amount >= 0
"""
)

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ------------------------------------------------------------------------------
# TARGET: silver.data_quality_daily_silver
# ------------------------------------------------------------------------------

data_quality_daily_silver_df = spark.sql(
    """
SELECT
  run_date,
  dataset_name,
  records_processed_count,
  duplicate_records_removed_count,
  invalid_identifier_count
FROM (
  SELECT
    CAST(CURRENT_DATE AS DATE) AS run_date,
    x.dataset_name,
    x.records_processed_count,
    x.duplicate_records_removed_count,
    x.invalid_identifier_count
  FROM (
    SELECT
      'products_bronze' AS dataset_name,
      (SELECT COUNT(*) FROM bronze.products_bronze) AS records_processed_count,
      (SELECT COUNT(*) FROM bronze.products_bronze)
        - (SELECT COUNT(*)
           FROM (
             SELECT
               pb.product_id,
               ROW_NUMBER() OVER (PARTITION BY pb.product_id ORDER BY pb.product_id) AS rn
             FROM bronze.products_bronze pb
             WHERE pb.product_id IS NOT NULL
           ) d
           WHERE d.rn = 1
        ) AS duplicate_records_removed_count,
      (SELECT COUNT(*) FROM bronze.products_bronze pb WHERE pb.product_id IS NULL) AS invalid_identifier_count
    UNION ALL
    SELECT
      'stores_bronze' AS dataset_name,
      (SELECT COUNT(*) FROM bronze.stores_bronze) AS records_processed_count,
      (SELECT COUNT(*) FROM bronze.stores_bronze)
        - (SELECT COUNT(*)
           FROM (
             SELECT
               sb.store_id,
               ROW_NUMBER() OVER (PARTITION BY sb.store_id ORDER BY sb.store_id) AS rn
             FROM bronze.stores_bronze sb
             WHERE sb.store_id IS NOT NULL
           ) d
           WHERE d.rn = 1
        ) AS duplicate_records_removed_count,
      (SELECT COUNT(*) FROM bronze.stores_bronze sb WHERE sb.store_id IS NULL) AS invalid_identifier_count
    UNION ALL
    SELECT
      'sales_transactions_bronze' AS dataset_name,
      (SELECT COUNT(*) FROM bronze.sales_transactions_bronze) AS records_processed_count,
      (SELECT COUNT(*) FROM bronze.sales_transactions_bronze)
        - (SELECT COUNT(*)
           FROM (
             SELECT
               stb.transaction_id,
               ROW_NUMBER() OVER (PARTITION BY stb.transaction_id ORDER BY stb.transaction_time DESC) AS rn
             FROM bronze.sales_transactions_bronze stb
             WHERE stb.transaction_id IS NOT NULL
           ) d
           WHERE d.rn = 1
        ) AS duplicate_records_removed_count,
      (SELECT COUNT(*)
       FROM bronze.sales_transactions_bronze stb
       WHERE stb.transaction_id IS NULL
          OR stb.store_id IS NULL
          OR stb.product_id IS NULL
      ) AS invalid_identifier_count
  ) x
) q
"""
)

(
    data_quality_daily_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_quality_daily_silver.csv")
)

job.commit()
