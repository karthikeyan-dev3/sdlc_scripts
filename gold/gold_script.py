```python
import sys
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Spark

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# ------------------------------------------------------------------------------
# 1) Read source tables from S3 (CSV)
# ------------------------------------------------------------------------------
product_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_details_silver.{FILE_FORMAT}/")
)

store_details_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_details_silver.{FILE_FORMAT}/")
)

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)

aggregated_sales_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/aggregated_sales_silver.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------------------
product_details_silver_df.createOrReplaceTempView("product_details_silver")
store_details_silver_df.createOrReplaceTempView("store_details_silver")
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
aggregated_sales_silver_df.createOrReplaceTempView("aggregated_sales_silver")

# ------------------------------------------------------------------------------
# 3) Transformations using Spark SQL
# ------------------------------------------------------------------------------

# ---- gold_product_master (gpm) ----
gold_product_master_df = spark.sql("""
SELECT
  CAST(pds.product_id AS STRING)  AS product_id,
  CAST(pds.product_name AS STRING) AS product_name,
  CAST(pds.category AS STRING)     AS category
FROM product_details_silver pds
""")

# ---- gold_store_master (gsm) ----
gold_store_master_df = spark.sql("""
SELECT
  CAST(sds.store_id AS STRING)   AS store_id,
  CAST(sds.store_name AS STRING) AS store_name,
  CAST(sds.region AS STRING)     AS region
FROM store_details_silver sds
""")

# ---- gold_sales (gs) ----
gold_sales_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING) AS transaction_id,
  DATE(sts.sale_date)               AS sale_date,
  CAST(sts.product_id AS STRING)    AS product_id,
  CAST(sts.store_id AS STRING)      AS store_id,
  CAST(sts.quantity_sold AS INT)    AS quantity_sold,
  CAST(sts.total_sales_value AS DOUBLE) AS total_sales_value,
  CAST(pds.product_name AS STRING)  AS product_name,
  CAST(sds.store_name AS STRING)    AS store_name
FROM sales_transactions_silver sts
LEFT JOIN product_details_silver pds
  ON sts.product_id = pds.product_id
LEFT JOIN store_details_silver sds
  ON sts.store_id = sds.store_id
""")

# ---- gold_aggregated_sales (gas) ----
gold_aggregated_sales_df = spark.sql("""
SELECT
  DATE(ass.aggregation_date)            AS aggregation_date,
  CAST(ass.total_quantity_sold AS INT)  AS total_quantity_sold,
  CAST(ass.total_sales_value AS DOUBLE) AS total_sales_value,
  CAST(ass.average_sales_value AS DOUBLE) AS average_sales_value
FROM aggregated_sales_silver ass
""")

# ------------------------------------------------------------------------------
# 4) Write each target table separately as a SINGLE CSV file directly under TARGET_PATH
# ------------------------------------------------------------------------------
(
    gold_product_master_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_master.csv")
)

(
    gold_store_master_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_master.csv")
)

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format(FILE_FORMAT)
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()
```