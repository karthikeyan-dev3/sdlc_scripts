import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, [])
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# Read Source Tables from S3 (Silver Layer)
# ------------------------------------------------------------------------------------
sts_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
ps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)
ss_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------------
# Create Temp Views
# ------------------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sts")
ps_df.createOrReplaceTempView("ps")
ss_df.createOrReplaceTempView("ss")

# ------------------------------------------------------------------------------------
# Target: gold_sales_transactions
# ------------------------------------------------------------------------------------
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  DATE(sts.transaction_date) AS transaction_date,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.total_sales AS DOUBLE) AS total_sales
FROM sts
""")

(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_product_master
# ------------------------------------------------------------------------------------
gold_product_master_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING) AS category,
  CAST(ps.brand AS STRING) AS brand,
  CAST(ps.price AS FLOAT) AS price
FROM ps
""")

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_store_master
# ------------------------------------------------------------------------------------
gold_store_master_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.city AS STRING) AS location,
  CAST(ss.store_type AS STRING) AS store_type
FROM ss
""")

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_sales_aggregated
# ------------------------------------------------------------------------------------
gold_sales_aggregated_df = spark.sql("""
SELECT
  DATE(sts.transaction_date) AS date,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(ps.category AS STRING) AS category,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT) AS total_quantity_sold,
  CAST(SUM(CAST(sts.total_sales AS DOUBLE)) AS DOUBLE) AS total_sales
FROM sts
INNER JOIN ps
  ON sts.product_id = ps.product_id
GROUP BY
  DATE(sts.transaction_date),
  CAST(sts.store_id AS STRING),
  CAST(ps.category AS STRING)
""")

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_cleaned_data
# ------------------------------------------------------------------------------------
gold_cleaned_data_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  DATE(sts.transaction_date) AS cleaned_date,
  CAST(sts.quantity_sold AS INT) AS cleaned_quantity,
  CAST(sts.total_sales AS DOUBLE) AS cleaned_sales
FROM sts
""")

(
    gold_cleaned_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_cleaned_data.csv")
)

# ------------------------------------------------------------------------------------
# Target: gold_sales_summary
# ------------------------------------------------------------------------------------
gold_sales_summary_df = spark.sql("""
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(SUM(CAST(sts.total_sales AS DOUBLE)) AS DOUBLE) AS monthly_sales,
  CAST(
    SUM(CAST(sts.total_sales AS DOUBLE)) /
    NULLIF(SUM(CAST(sts.quantity_sold AS DOUBLE)), 0.0)
    AS DOUBLE
  ) AS average_price
FROM sts
INNER JOIN ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(sts.store_id AS STRING),
  CAST(sts.product_id AS STRING)
""")

(
    gold_sales_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_summary.csv")
)