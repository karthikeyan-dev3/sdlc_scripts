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

# ------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------
sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

products_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/products_silver.{FILE_FORMAT}/")
)

stores_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/stores_silver.{FILE_FORMAT}/")
)

aggregated_sales_daily_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_daily_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
aggregated_sales_daily_silver_df.createOrReplaceTempView("aggregated_sales_daily_silver")

# ------------------------------------------------------------
# 3) Transformations using Spark SQL
# ------------------------------------------------------------

# gold.gold_sales_transactions (gst) from silver.sales_transactions_silver (sts)
gold_sales_transactions_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  CAST(sts.sale_date AS DATE) AS sale_date,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.quantity_sold AS INT) AS quantity_sold,
  CAST(sts.total_sale_amount AS DOUBLE) AS total_sale_amount
FROM sales_transactions_silver sts
""")

# gold.gold_product_master (gpm) from silver.products_silver (ps)
gold_product_master_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING) AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING) AS category,
  CAST(ps.brand AS STRING) AS brand
FROM products_silver ps
""")

# gold.gold_store_master (gsm) from silver.stores_silver (ss)
gold_store_master_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.location AS STRING) AS location,
  CAST(ss.region AS STRING) AS region
FROM stores_silver ss
""")

# gold.gold_aggregated_sales (gas) from silver.aggregated_sales_daily_silver (asds)
gold_aggregated_sales_df = spark.sql("""
SELECT
  CAST(asds.store_id AS STRING) AS store_id,
  CAST(asds.product_id AS STRING) AS product_id,
  CAST(asds.total_quantity_sold AS BIGINT) AS total_quantity_sold,
  CAST(asds.total_revenue AS DOUBLE) AS total_revenue,
  CAST(asds.aggregation_date AS DATE) AS aggregation_date
FROM aggregated_sales_daily_silver asds
""")

# ------------------------------------------------------------
# 4) Save outputs (single CSV file directly under TARGET_PATH)
# ------------------------------------------------------------
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
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()