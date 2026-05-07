import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# ------------------------------------------------------------------------------------
# 1) Read source tables
# ------------------------------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.csv/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# ------------------------------------------------------------------------------------
# TARGET: sales_daily_product_store_gold
# ------------------------------------------------------------------------------------
sales_daily_product_store_gold_sql = """
SELECT
  CAST(sts.transaction_date AS DATE) AS transaction_date,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(SUM(sts.quantity_sold) AS INT) AS quantity_sold,
  CAST(SUM(sts.total_revenue) AS DOUBLE) AS total_revenue,
  CAST(
    CASE
      WHEN SUM(sts.quantity_sold) > 0 THEN SUM(sts.total_revenue) / SUM(sts.quantity_sold)
    END
    AS DOUBLE
  ) AS avg_price_per_unit
FROM sales_transactions_silver sts
GROUP BY
  sts.transaction_date,
  sts.product_id,
  sts.store_id
"""
sales_daily_product_store_gold_df = spark.sql(sales_daily_product_store_gold_sql)

(
    sales_daily_product_store_gold_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_daily_product_store_gold.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: sales_product_gold
# ------------------------------------------------------------------------------------
sales_product_gold_sql = """
SELECT
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(SUM(sts.quantity_sold) AS INT) AS quantity_sold,
  CAST(SUM(sts.total_revenue) AS DOUBLE) AS total_revenue,
  CAST(
    CASE
      WHEN SUM(sts.quantity_sold) > 0 THEN SUM(sts.total_revenue) / SUM(sts.quantity_sold)
    END
    AS DOUBLE
  ) AS avg_price_per_unit
FROM sales_transactions_silver sts
GROUP BY
  sts.product_id
"""
sales_product_gold_df = spark.sql(sales_product_gold_sql)

(
    sales_product_gold_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_product_gold.csv")
)

# ------------------------------------------------------------------------------------
# TARGET: sales_store_gold
# ------------------------------------------------------------------------------------
sales_store_gold_sql = """
SELECT
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(SUM(sts.quantity_sold) AS INT) AS quantity_sold,
  CAST(SUM(sts.total_revenue) AS DOUBLE) AS total_revenue,
  CAST(
    CASE
      WHEN SUM(sts.quantity_sold) > 0 THEN SUM(sts.total_revenue) / SUM(sts.quantity_sold)
    END
    AS DOUBLE
  ) AS avg_price_per_unit
FROM sales_transactions_silver sts
GROUP BY
  sts.store_id
"""
sales_store_gold_df = spark.sql(sales_store_gold_sql)

(
    sales_store_gold_df.coalesce(1)
    .write.mode("overwrite")
    .option("header", "true")
    .csv(f"{TARGET_PATH}/sales_store_gold.csv")
)

job.commit()
