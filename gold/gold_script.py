import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -------------------------------
# 1) Read source tables from S3
# -------------------------------
sps_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_performance_silver.{FILE_FORMAT}/")
)

pms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_master_silver.{FILE_FORMAT}/")
)

sms_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_master_silver.{FILE_FORMAT}/")
)

# -------------------------------
# 2) Create temp views
# -------------------------------
sps_df.createOrReplaceTempView("sales_performance_silver")
pms_df.createOrReplaceTempView("product_master_silver")
sms_df.createOrReplaceTempView("store_master_silver")

# -------------------------------
# 3) Transform + 4) Save outputs
# -------------------------------

# gold.gold_sales_performance
gold_sales_performance_df = spark.sql(
    """
SELECT
  CAST(sps.transaction_id AS STRING)                 AS transaction_id,
  CAST(sps.sale_date AS DATE)                        AS sale_date,
  CAST(sps.product_id AS STRING)                     AS product_id,
  CAST(sps.store_id AS STRING)                       AS store_id,
  CAST(sps.quantity_sold AS INT)                     AS quantity_sold,
  CAST(sps.sales_amount AS DECIMAL(18,2))            AS sales_amount
FROM sales_performance_silver sps
"""
)

(
    gold_sales_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_sales_performance.csv")
)

# gold.gold_product_master
gold_product_master_df = spark.sql(
    """
SELECT
  CAST(pms.product_id AS STRING)                     AS product_id,
  CAST(pms.product_name AS STRING)                   AS product_name,
  CAST(pms.category AS STRING)                       AS category,
  CAST(pms.price AS DECIMAL(18,2))                   AS price,
  CAST(pms.manufacturer AS STRING)                   AS manufacturer
FROM product_master_silver pms
"""
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_product_master.csv")
)

# gold.gold_store_master
gold_store_master_df = spark.sql(
    """
SELECT
  CAST(sms.store_id AS STRING)                       AS store_id,
  CAST(sms.store_name AS STRING)                     AS store_name,
  CAST(sms.location AS STRING)                       AS location,
  CAST(sms.region AS STRING)                         AS region,
  CAST(sms.store_type AS STRING)                     AS store_type
FROM store_master_silver sms
"""
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_store_master.csv")
)

# gold.gold_aggregated_report
gold_aggregated_report_df = spark.sql(
    """
SELECT
  CAST(sps.sale_date AS DATE)                        AS aggregation_date,
  CAST(sps.store_id AS STRING)                       AS store_id,
  CAST(sps.product_id AS STRING)                     AS product_id,
  CAST(SUM(CAST(sps.sales_amount AS DECIMAL(18,2))) AS DECIMAL(18,2)) AS total_sales_amount,
  CAST(SUM(CAST(sps.quantity_sold AS INT)) AS INT)   AS total_quantity_sold,
  CAST(
    SUM(CAST(sps.sales_amount AS DECIMAL(18,2))) / SUM(CAST(sps.quantity_sold AS INT))
    AS DECIMAL(18,2)
  )                                                  AS average_price
FROM sales_performance_silver sps
LEFT JOIN product_master_silver pms
  ON sps.product_id = pms.product_id
LEFT JOIN store_master_silver sms
  ON sps.store_id = sms.store_id
GROUP BY
  sps.sale_date,
  sps.store_id,
  sps.product_id
"""
)

(
    gold_aggregated_report_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(TARGET_PATH + "/gold_aggregated_report.csv")
)

job.commit()
