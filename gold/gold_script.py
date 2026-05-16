```python
import sys
from awsglue.context import GlueConte
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import SparkSession

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# -----------------------------------------------------------------------------------
# Read Source Tables (S3 -> Spark DataFrames) and Create Temp Views
# -----------------------------------------------------------------------------------

product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/product_silver.{FILE_FORMAT}/")
)
product_silver_df.createOrReplaceTempView("product_silver")

store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/store_silver.{FILE_FORMAT}/")
)
store_silver_df.createOrReplaceTempView("store_silver")

sales_transactions_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_transactions_silver.{FILE_FORMAT}/")
)
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold_product
# Mapping: silver.product_silver ps
# -----------------------------------------------------------------------------------

gold_product_df = spark.sql("""
SELECT
  CAST(ps.product_id AS STRING)   AS product_id,
  CAST(ps.product_name AS STRING) AS product_name,
  CAST(ps.category AS STRING)     AS category,
  CAST(ps.brand AS STRING)        AS brand,
  CAST(ps.price AS DOUBLE)        AS price
FROM product_silver ps
""")

gold_product_output_path = f"{TARGET_PATH}/gold_product.csv"
(
    gold_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_product_output_path)
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold_store
# Mapping: silver.store_silver ss
# -----------------------------------------------------------------------------------

gold_store_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING)     AS store_id,
  CAST(ss.store_name AS STRING)   AS store_name,
  CAST(ss.location AS STRING)     AS location,
  CAST(ss.store_type AS STRING)   AS store_type
FROM store_silver ss
""")

gold_store_output_path = f"{TARGET_PATH}/gold_store.csv"
(
    gold_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_store_output_path)
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold_sales
# Mapping: silver.sales_transactions_silver sts
#          LEFT JOIN silver.product_silver ps ON sts.product_id = ps.product_id
#          LEFT JOIN silver.store_silver ss ON sts.store_id = ss.store_id
# -----------------------------------------------------------------------------------

gold_sales_df = spark.sql("""
SELECT
  CAST(sts.transaction_id AS STRING)  AS transaction_id,
  CAST(sts.product_id AS STRING)      AS product_id,
  CAST(sts.store_id AS STRING)        AS store_id,
  DATE(sts.sale_date)                 AS sale_date,
  CAST(sts.quantity_sold AS INT)      AS quantity_sold,
  CAST(sts.total_sale_amount AS DOUBLE) AS total_sale_amount
FROM sales_transactions_silver sts
LEFT JOIN product_silver ps
  ON sts.product_id = ps.product_id
LEFT JOIN store_silver ss
  ON sts.store_id = ss.store_id
""")

gold_sales_output_path = f"{TARGET_PATH}/gold_sales.csv"
(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_sales_output_path)
)

# -----------------------------------------------------------------------------------
# TARGET TABLE: gold_sales_aggregated
# Mapping: silver.sales_transactions_silver sts
# -----------------------------------------------------------------------------------

gold_sales_aggregated_df = spark.sql("""
SELECT
  CAST(sts.store_id AS STRING)     AS store_id,
  CAST(sts.product_id AS STRING)   AS product_id,
  DATE(sts.sale_date)              AS sale_date,
  CAST(SUM(CAST(sts.quantity_sold AS INT)) AS INT)       AS total_quantity_sold,
  CAST(SUM(CAST(sts.total_sale_amount AS DOUBLE)) AS DOUBLE) AS total_sales_amount
FROM sales_transactions_silver sts
GROUP BY
  CAST(sts.store_id AS STRING),
  CAST(sts.product_id AS STRING),
  DATE(sts.sale_date)
""")

gold_sales_aggregated_output_path = f"{TARGET_PATH}/gold_sales_aggregated.csv"
(
    gold_sales_aggregated_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(gold_sales_aggregated_output_path)
)

job.commit()
```