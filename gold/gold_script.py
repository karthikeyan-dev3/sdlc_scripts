```python
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkCon
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------
# Read source tables (S3)
# -------------------------
sales_transactions_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

product_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

store_master_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

sales_daily_store_product_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_daily_store_product_silver.{FILE_FORMAT}/")
)

sales_performance_store_product_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_performance_store_product_silver.{FILE_FORMAT}/")
)

# -------------------------
# Create temp views
# -------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
product_master_silver_df.createOrReplaceTempView("product_master_silver")
store_master_silver_df.createOrReplaceTempView("store_master_silver")
sales_daily_store_product_silver_df.createOrReplaceTempView("sales_daily_store_product_silver")
sales_performance_store_product_silver_df.createOrReplaceTempView("sales_performance_store_product_silver")

# =========================================================
# Target: gold_sales_transactions
# =========================================================
gold_sales_transactions_df = spark.sql("""
WITH base AS (
  SELECT
    CAST(sts.transaction_id AS STRING) AS transaction_id,
    CAST(sts.transaction_date AS DATE) AS transaction_date,
    CAST(sts.store_id AS STRING) AS store_id,
    CAST(sts.product_id AS STRING) AS product_id,
    CAST(sts.quantity_sold AS INT) AS quantity_sold,
    CAST(sts.total_sales_amount AS DOUBLE) AS total_sales_amount
  FROM sales_transactions_silver sts
),
dedup AS (
  SELECT
    transaction_id,
    transaction_date,
    store_id,
    product_id,
    quantity_sold,
    total_sales_amount,
    ROW_NUMBER() OVER (
      PARTITION BY transaction_id
      ORDER BY transaction_id
    ) AS rn
  FROM base
)
SELECT
  transaction_id,
  transaction_date,
  store_id,
  product_id,
  quantity_sold,
  total_sales_amount
FROM dedup
WHERE rn = 1
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# =========================================================
# Target: gold_product_master
# =========================================================
gold_product_master_df = spark.sql("""
SELECT
  CAST(pms.product_id AS STRING) AS product_id,
  CAST(pms.product_name AS STRING) AS product_name,
  CAST(pms.category AS STRING) AS category,
  CAST(pms.brand AS STRING) AS brand,
  CAST(pms.price AS FLOAT) AS price
FROM product_master_silver pms
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# =========================================================
# Target: gold_store_master
# =========================================================
gold_store_master_df = spark.sql("""
SELECT
  CAST(sms.store_id AS STRING) AS store_id,
  CAST(sms.store_name AS STRING) AS store_name,
  CAST(sms.location AS STRING) AS location
FROM store_master_silver sms
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# =========================================================
# Target: gold_sales_aggregated
# =========================================================
gold_sales_aggregated_df = spark.sql("""
SELECT
  CAST(sdsp.store_id AS STRING) AS store_id,
  CAST(sdsp.product_id AS STRING) AS product_id,
  CAST(sdsp.date AS DATE) AS date,
  CAST(sdsp.total_quantity_sold AS BIGINT) AS total_quantity_sold,
  CAST(sdsp.total_sales_amount AS DOUBLE) AS total_sales_amount
FROM sales_daily_store_product_silver sdsp
""")

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# =========================================================
# Target: gold_sales_performance
# =========================================================
gold_sales_performance_df = spark.sql("""
SELECT
  CAST(spsp.store_id AS STRING) AS store_id,
  CAST(spsp.product_id AS STRING) AS product_id,
  CAST(spsp.weekly_sales_volume AS BIGINT) AS weekly_sales_volume,
  CAST(spsp.monthly_sales_volume AS BIGINT) AS monthly_sales_volume,
  CAST(spsp.average_unit_price AS DOUBLE) AS average_unit_price
FROM sales_performance_store_product_silver spsp
""")

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_performance.csv")
)

job.commit()
```