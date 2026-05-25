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

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# ------------------------------------------------------------
# Target Table: gold_sales
# ------------------------------------------------------------
gold_sales_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)            AS transaction_id,
  CAST(sts.product_id AS STRING)                AS product_id,
  CAST(sts.store_id AS STRING)                  AS store_id,
  CAST(sts.sale_date AS DATE)                   AS sale_date,
  CAST(sts.quantity_sold AS INT)                AS quantity_sold,
  CAST(sts.total_sales_amount AS DOUBLE)        AS total_sales_amount,
  CAST(ps.product_name AS STRING)               AS product_name,
  CAST(ps.category AS STRING)                   AS category,
  CAST(ss.store_location AS STRING)             AS store_location,
  CAST(ss.store_type AS STRING)                 AS store_type
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY sts.transaction_id
  ORDER BY sts.transaction_id
) = 1
""")

(
    gold_sales_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

# ------------------------------------------------------------
# Target Table: gold_aggregated_sales
# ------------------------------------------------------------
gold_aggregated_sales_df = spark.sql("""
SELECT
  CAST(sts.sale_date AS DATE)                                                   AS aggregation_date,
  CAST(ss.store_id AS STRING)                                                   AS store_id,
  CAST(ss.region AS STRING)                                                     AS region,
  CAST(SUM(CAST(sts.total_sales_amount AS DOUBLE)) AS DOUBLE)                   AS total_sales,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT)                              AS total_quantity,
  CAST(
    SUM(CAST(sts.total_sales_amount AS DOUBLE)) / COUNT(DISTINCT sts.transaction_id)
    AS DOUBLE
  )                                                                             AS average_sales_per_transaction
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  CAST(sts.sale_date AS DATE),
  CAST(ss.store_id AS STRING),
  CAST(ss.region AS STRING)
""")

(
    gold_aggregated_sales_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()