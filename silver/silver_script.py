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

# =========================
# 1) Read source tables
# =========================
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

# =========================
# 2) Create temp views
# =========================
products_bronze_df.createOrReplaceTempView("products_bronze")
stores_bronze_df.createOrReplaceTempView("stores_bronze")
sales_transactions_bronze_df.createOrReplaceTempView("sales_transactions_bronze")

# ============================================================
# TABLE: silver.product_master_silver (pms)
# ============================================================
product_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(pb.product_id) AS product_id,
    TRIM(pb.product_name) AS product_name,
    TRIM(pb.category) AS category,
    TRIM(pb.brand) AS brand,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(pb.product_id)
      ORDER BY TRIM(pb.product_id)
    ) AS rn
  FROM products_bronze pb
  WHERE TRIM(pb.product_id) IS NOT NULL
    AND TRIM(pb.product_id) <> ''
)
SELECT
  product_id,
  product_name,
  category,
  brand
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
# TABLE: silver.store_master_silver (sms)
# ============================================================
store_master_silver_df = spark.sql("""
WITH base AS (
  SELECT
    TRIM(sb.store_id) AS store_id,
    CONCAT(TRIM(sb.city), ',', TRIM(sb.state)) AS store_location,
    TRIM(sb.store_type) AS store_size,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(sb.store_id)
      ORDER BY TRIM(sb.store_id)
    ) AS rn
  FROM stores_bronze sb
  WHERE TRIM(sb.store_id) IS NOT NULL
    AND TRIM(sb.store_id) <> ''
)
SELECT
  store_id,
  store_location,
  store_size
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
# TABLE: silver.sales_transactions_clean_silver (stcs)
# ============================================================
sales_transactions_clean_silver_df = spark.sql("""
WITH dedup AS (
  SELECT
    TRIM(stb.transaction_id) AS transaction_id,
    CAST(stb.transaction_time AS date) AS sale_date,
    TRIM(stb.product_id) AS product_id,
    TRIM(stb.store_id) AS store_id,
    CAST(stb.quantity AS int) AS quantity_sold,
    CAST(stb.sale_amount AS double) AS total_amount,
    ROW_NUMBER() OVER (
      PARTITION BY TRIM(stb.transaction_id)
      ORDER BY TRIM(stb.transaction_id)
    ) AS rn
  FROM sales_transactions_bronze stb
  WHERE TRIM(stb.transaction_id) IS NOT NULL
    AND TRIM(stb.transaction_id) <> ''
),
filtered AS (
  SELECT
    d.transaction_id,
    d.sale_date,
    d.product_id,
    d.store_id,
    d.quantity_sold,
    d.total_amount
  FROM dedup d
  LEFT JOIN product_master_silver pms
    ON d.product_id = pms.product_id
  LEFT JOIN store_master_silver sms
    ON d.store_id = sms.store_id
  WHERE d.rn = 1
    AND pms.product_id IS NOT NULL
    AND sms.store_id IS NOT NULL
    AND d.quantity_sold >= 0
    AND d.total_amount >= 0
)
SELECT
  transaction_id,
  sale_date,
  product_id,
  store_id,
  quantity_sold,
  total_amount
FROM filtered
""")

sales_transactions_clean_silver_df.createOrReplaceTempView("sales_transactions_clean_silver")

(
    sales_transactions_clean_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/sales_transactions_clean_silver.csv")
)

# ============================================================
# TABLE: silver.aggregated_sales_silver (ass)
# ============================================================
aggregated_sales_silver_df = spark.sql("""
SELECT
  stcs.store_id AS store_id,
  stcs.product_id AS product_id,
  SUM(stcs.total_amount) AS total_sales,
  SUM(stcs.quantity_sold) AS total_quantity_sold,
  SUM(stcs.total_amount) / NULLIF(SUM(stcs.quantity_sold), 0) AS average_sale_price
FROM sales_transactions_clean_silver stcs
GROUP BY
  stcs.store_id,
  stcs.product_id
""")

(
    aggregated_sales_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/aggregated_sales_silver.csv")
)

# ============================================================
# TABLE: silver.kpi_metrics_silver (kms)
# ============================================================
kpi_metrics_silver_df = spark.sql("""
SELECT
  'total_records_cleaned' AS metric_name,
  CAST(COUNT(stcs.transaction_id) AS double) AS metric_value,
  CAST(COUNT(stcs.transaction_id) AS double) AS target_value
FROM sales_transactions_clean_silver stcs
LEFT JOIN sales_transactions_bronze stb
  ON stcs.transaction_id = TRIM(stb.transaction_id)
""")

(
    kpi_metrics_silver_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/kpi_metrics_silver.csv")
)

job.commit()