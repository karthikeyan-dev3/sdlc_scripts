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

# -----------------------------
# 1) Read source tables (Bronze)
# -----------------------------
sales_transactions_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_bronze.{FILE_FORMAT}/")
)

product_details_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_bronze.{FILE_FORMAT}/")
)

store_information_bronze_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_information_bronze.{FILE_FORMAT}/")
)

# -----------------------------
# 2) Create temp views
# -----------------------------
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")
product_details_bronze_df.createOrReplaceTempView("product_details_bronze")
store_information_bronze_df.createOrReplaceTempView("store_information_bronze")

# ============================================================
# TARGET: silver.sales_transactions_silver
# Source: bronze.sales_transactions_bronze stb
# ============================================================
sales_transactions_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(stb.transaction_id AS string) AS transaction_id,
    CAST(stb.store_id AS string) AS store_id,
    CAST(stb.product_id AS string) AS product_id,
    CAST(stb.quantity AS int) AS quantity_sold,
    CAST(stb.sale_amount AS double) AS total_sale_amount,
    CAST(CAST(stb.transaction_time AS timestamp) AS date) AS sale_date,
    CAST(stb.transaction_time AS timestamp) AS transaction_time
  FROM sales_transactions_bronze stb
  WHERE
    stb.transaction_id IS NOT NULL
    AND stb.store_id IS NOT NULL
    AND stb.product_id IS NOT NULL
    AND CAST(stb.quantity AS int) > 0
    AND CAST(stb.sale_amount AS double) >= 0
),
dedup AS (
  SELECT
    transaction_id,
    store_id,
    product_id,
    quantity_sold,
    total_sale_amount,
    sale_date,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_time DESC
    ) AS rn
  FROM base
)
SELECT
  transaction_id,
  store_id,
  product_id,
  quantity_sold,
  total_sale_amount,
  sale_date
FROM dedup
WHERE rn = 1
""")

sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

(
    sales_transactions_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_silver.csv")
)

# ============================================================
# TARGET: silver.product_master_silver
# Source: bronze.product_details_bronze pdb
# ============================================================
product_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(pdb.product_id AS string) AS product_id,
    TRIM(pdb.product_name) AS product_name,
    TRIM(pdb.category) AS product_category,
    CAST(pdb.price AS float) AS price,
    ROW_NUMBER() OVER (
      PARTITION BY pdb.product_id
      ORDER BY pdb.product_id
    ) AS rn
  FROM product_details_bronze pdb
  WHERE
    pdb.product_id IS NOT NULL
    AND pdb.product_name IS NOT NULL
    AND pdb.category IS NOT NULL
    AND pdb.price IS NOT NULL
    AND CAST(pdb.is_active AS boolean) = true
)
SELECT
  product_id,
  product_name,
  product_category,
  price
FROM base
WHERE rn = 1
""")

product_master_silver_df.createOrReplaceTempView("product_master_silver")

(
    product_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/product_master_silver.csv")
)

# ============================================================
# TARGET: silver.store_master_silver
# Source: bronze.store_information_bronze sib
# ============================================================
store_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(sib.store_id AS string) AS store_id,
    TRIM(sib.store_name) AS store_name,
    CONCAT(TRIM(sib.city), ', ', TRIM(sib.state)) AS store_location,
    CAST(sib.store_type AS string) AS store_type,
    ROW_NUMBER() OVER (
      PARTITION BY sib.store_id
      ORDER BY sib.store_id
    ) AS rn
  FROM store_information_bronze sib
  WHERE
    sib.store_id IS NOT NULL
    AND sib.store_name IS NOT NULL
    AND sib.city IS NOT NULL
    AND sib.state IS NOT NULL
    AND sib.store_type IS NOT NULL
)
SELECT
  store_id,
  store_name,
  store_location,
  store_type
FROM base
WHERE rn = 1
""")

store_master_silver_df.createOrReplaceTempView("store_master_silver")

(
    store_master_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/store_master_silver.csv")
)

# ============================================================
# TARGET: silver.sales_aggregated_silver
# Source: silver.sales_transactions_silver sts
#         INNER JOIN silver.store_master_silver sms ON sts.store_id = sms.store_id
# ============================================================
sales_aggregated_silver_df = spark.sql("""
SELECT
  sts.sale_date AS aggregation_date,
  sts.store_id AS store_id,
  SUM(sts.total_sale_amount) AS total_sales_amount,
  SUM(sts.quantity_sold) AS total_quantity_sold,
  AVG(sts.total_sale_amount) AS average_sale_per_transaction
FROM sales_transactions_silver sts
INNER JOIN store_master_silver sms
  ON sts.store_id = sms.store_id
GROUP BY
  sts.sale_date,
  sts.store_id
""")

sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

(
    sales_aggregated_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_aggregated_silver.csv")
)

# ============================================================
# TARGET: silver.data_refresh_log_silver
# Source: silver.sales_transactions_silver sts
#         CROSS JOIN (SELECT 'sales_transactions_raw' AS data_source UNION ALL SELECT 'products_raw' UNION ALL SELECT 'stores_raw') src
# ============================================================
data_refresh_log_silver_df = spark.sql("""
WITH src AS (
  SELECT 'sales_transactions_raw' AS data_source
  UNION ALL
  SELECT 'products_raw' AS data_source
  UNION ALL
  SELECT 'stores_raw' AS data_source
),
agg AS (
  SELECT
    CURRENT_DATE() AS refresh_date,
    src.data_source AS data_source,
    CASE WHEN COUNT(sts.transaction_id) > 0 THEN 'SUCCESS' ELSE 'FAILED' END AS status
  FROM sales_transactions_silver sts
  CROSS JOIN src
  GROUP BY
    CURRENT_DATE(),
    src.data_source
)
SELECT
  refresh_date,
  data_source,
  status,
  MAX(CASE WHEN status = 'SUCCESS' THEN refresh_date END) OVER (PARTITION BY data_source) AS last_successful_refresh
FROM agg
""")

data_refresh_log_silver_df.createOrReplaceTempView("data_refresh_log_silver")

(
    data_refresh_log_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/data_refresh_log_silver.csv")
)

job.commit()