import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark: SparkSession = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------------
# 1) Read source tables from S3
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
# 2) Create temp views
# ------------------------------------------------------------------------------------
sts_df.createOrReplaceTempView("sts")
ps_df.createOrReplaceTempView("ps")
ss_df.createOrReplaceTempView("ss")

# ------------------------------------------------------------------------------------
# 3) Transformations using Spark SQL
# ------------------------------------------------------------------------------------

# gold.gold_sales_transactions (gst)
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)  AS transaction_id,
  CAST(sts.transaction_date AS DATE)  AS transaction_date,
  CAST(sts.product_id AS STRING)      AS product_id,
  CAST(sts.store_id AS STRING)        AS store_id,
  CAST(sts.quantity_sold AS INT)      AS quantity_sold,
  CAST(sts.total_sales AS DOUBLE)     AS total_sales
FROM sts
""")

# gold.gold_product_master (gpm)
gold_product_master_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)     AS product_id,
  CAST(ps.product_name AS STRING)   AS product_name,
  CAST(ps.category AS STRING)       AS category,
  CAST(ps.brand AS STRING)          AS brand,
  CAST(ps.price AS FLOAT)           AS price
FROM ps
""")

# gold.gold_store_master (gsm)
gold_store_master_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING)     AS store_id,
  CAST(ss.store_name AS STRING)   AS store_name,
  CAST(ss.city AS STRING)         AS location,
  CAST(ss.store_type AS STRING)   AS store_type
FROM ss
""")

# gold.gold_sales_aggregated (gsa)
gold_sales_aggregated_df = spark.sql("""
SELECT
  CAST(sts.transaction_date AS DATE) AS date,
  CAST(sts.store_id AS STRING)       AS store_id,
  CAST(ps.category AS STRING)        AS category,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT)     AS total_quantity_sold,
  CAST(SUM(CAST(sts.total_sales AS DOUBLE)) AS DOUBLE) AS total_sales
FROM sts
INNER JOIN ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(sts.transaction_date AS DATE),
  CAST(sts.store_id AS STRING),
  CAST(ps.category AS STRING)
""")

# gold.gold_cleaned_data (gcd)
gold_cleaned_data_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)   AS transaction_id,
  CAST(sts.transaction_date AS DATE)   AS cleaned_date,
  CAST(sts.quantity_sold AS INT)       AS cleaned_quantity,
  CAST(sts.total_sales AS DOUBLE)      AS cleaned_sales
FROM sts
""")

# gold.gold_sales_summary (gss)
gold_sales_summary_df = spark.sql("""
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(SUM(CAST(sts.total_sales AS DOUBLE)) AS DOUBLE) AS monthly_sales,
  CAST(
    SUM(CAST(sts.total_sales AS DOUBLE)) / SUM(CAST(sts.quantity_sold AS DOUBLE))
    AS DOUBLE
  ) AS average_price
FROM sts
INNER JOIN ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(sts.store_id AS STRING),
  CAST(sts.product_id AS STRING),
  YEAR(CAST(sts.transaction_date AS DATE)),
  MONTH(CAST(sts.transaction_date AS DATE))
""")

# ------------------------------------------------------------------------------------
# 4) Save outputs (single CSV file per target table directly under TARGET_PATH)
# ------------------------------------------------------------------------------------
(
    gold_sales_transactions_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transactions.csv")
)

(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_aggregated.csv")
)

(
    gold_cleaned_data_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_cleaned_data.csv")
)

(
    gold_sales_summary_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_summary.csv")
)

job.commit()
