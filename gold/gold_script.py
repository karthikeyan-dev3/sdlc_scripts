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

# -------------------------
# Read Source Tables (S3)
# -------------------------
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

sales_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_silver.{FILE_FORMAT}/")
)

sales_aggregated_silver_df = (
    spark.read
    .format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_aggregated_silver.{FILE_FORMAT}/")
)

# -------------------------
# Create Temp Views
# -------------------------
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")
sales_silver_df.createOrReplaceTempView("sales_silver")
sales_aggregated_silver_df.createOrReplaceTempView("sales_aggregated_silver")

# -------------------------
# gold_product
# -------------------------
gold_product_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)        AS product_id,
  CAST(ps.product_name AS STRING)      AS product_name,
  CAST(ps.category AS STRING)          AS category
FROM products_silver ps
""")

(
    gold_product_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product.csv")
)

# -------------------------
# gold_store
# -------------------------
gold_store_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING)      AS store_id,
  CAST(ss.store_name AS STRING)    AS store_name
FROM stores_silver ss
""")

(
    gold_store_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store.csv")
)

# -------------------------
# gold_sales
# -------------------------
gold_sales_df = spark.sql("""
SELECT
  CAST(ssal.sale_id AS STRING)          AS sale_id,
  CAST(ssal.product_id AS STRING)       AS product_id,
  CAST(ssal.store_id AS STRING)         AS store_id,
  DATE(ssal.sale_date)                  AS sale_date,
  CAST(ssal.quantity_sold AS BIGINT)    AS quantity_sold,
  CAST(ssal.sales_amount AS DOUBLE)     AS sales_amount
FROM sales_silver ssal
LEFT JOIN products_silver ps
  ON ssal.product_id = ps.product_id
LEFT JOIN stores_silver ss
  ON ssal.store_id = ss.store_id
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

# -------------------------
# gold_sales_agg
# -------------------------
gold_sales_agg_df = spark.sql("""
SELECT
  DATE(sas.date)                           AS date,
  CAST(sas.total_sales_amount AS DOUBLE)   AS total_sales_amount,
  CAST(sas.total_quantity_sold AS BIGINT)  AS total_quantity_sold,
  CAST(sas.average_sales_amount AS DOUBLE) AS average_sales_amount,
  CAST(sas.number_of_sales AS BIGINT)      AS number_of_sales
FROM sales_aggregated_silver sas
""")

(
    gold_sales_agg_df
    .coalesce(1)
    .write
    .mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_agg.csv")
)

job.commit()