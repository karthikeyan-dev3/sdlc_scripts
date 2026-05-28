import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

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
# Read Source Tables (S3)
# -------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sas_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)
ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)
ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

# -------------------------
# Create Temp Views
# -------------------------
sts_df.createOrReplaceTempView("sts")
sas_df.createOrReplaceTempView("sas")
ss_df.createOrReplaceTempView("ss")
ps_df.createOrReplaceTempView("ps")

# ============================================================
# Target: gold_transaction_clean
# ============================================================
gold_transaction_clean_df = spark.sql(
    """
SELECT
  CAST(sts.transaction_id AS STRING)         AS transaction_id,
  CAST(sts.store_id AS STRING)               AS store_id,
  CAST(sts.product_id AS STRING)             AS product_id,
  CAST(sts.quantity_sold AS INT)             AS quantity_sold,
  CAST(sts.transaction_amount AS DOUBLE)     AS transaction_amount,
  CAST(sts.transaction_date AS DATE)         AS transaction_date
FROM (
  SELECT
    sts.*,
    ROW_NUMBER() OVER (
      PARTITION BY sts.transaction_id
      ORDER BY sts.transaction_id
    ) AS rn
  FROM sts
) sts
WHERE sts.rn = 1
"""
)

(
    gold_transaction_clean_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_transaction_clean.csv")
)

# ============================================================
# Target: gold_aggregated_sales
# ============================================================
gold_aggregated_sales_df = spark.sql(
    """
SELECT
  CAST(sas.store_id AS STRING)           AS store_id,
  CAST(sas.product_id AS STRING)         AS product_id,
  CAST(sas.total_sales AS DOUBLE)        AS total_sales,
  CAST(sas.total_quantity AS INT)        AS total_quantity,
  CAST(sas.aggregated_date AS DATE)      AS aggregated_date
FROM sas
"""
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

# ============================================================
# Target: gold_store_sales
# ============================================================
gold_store_sales_df = spark.sql(
    """
SELECT
  CAST(sas.store_id AS STRING)              AS store_id,
  CAST(ss.store_name AS STRING)             AS store_name,
  CAST(sas.aggregated_date AS DATE)         AS sales_date,
  CAST(SUM(CAST(sas.total_sales AS DOUBLE)) AS DOUBLE) AS total_revenue
FROM sas
INNER JOIN ss
  ON sas.store_id = ss.store_id
GROUP BY
  sas.store_id,
  ss.store_name,
  sas.aggregated_date
"""
)

(
    gold_store_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_sales.csv")
)

# ============================================================
# Target: gold_product_sales
# ============================================================
gold_product_sales_df = spark.sql(
    """
SELECT
  CAST(sas.product_id AS STRING)            AS product_id,
  CAST(ps.product_name AS STRING)           AS product_name,
  CAST(ps.category AS STRING)               AS category,
  CAST(sas.aggregated_date AS DATE)         AS sales_date,
  CAST(SUM(CAST(sas.total_sales AS DOUBLE)) AS DOUBLE) AS revenue_contribution
FROM sas
INNER JOIN ps
  ON sas.product_id = ps.product_id
GROUP BY
  sas.product_id,
  ps.product_name,
  ps.category,
  sas.aggregated_date
"""
)

(
    gold_product_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_sales.csv")
)

job.commit()
