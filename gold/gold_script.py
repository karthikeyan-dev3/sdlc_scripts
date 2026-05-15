```python
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkConte
from awsglue.context import GlueContext
from awsglue.job import Job
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

# ------------------------------------------------------------------
# 1) Read source tables from S3
# ------------------------------------------------------------------
silver_sales_transactions_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_sales_transactions.{FILE_FORMAT}/")
)

silver_stores_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_stores.{FILE_FORMAT}/")
)

silver_products_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_products.{FILE_FORMAT}/")
)

silver_daily_sales_summary_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/silver_daily_sales_summary.{FILE_FORMAT}/")
)

# ------------------------------------------------------------------
# 2) Create temp views
# ------------------------------------------------------------------
silver_sales_transactions_df.createOrReplaceTempView("silver_sales_transactions")
silver_stores_df.createOrReplaceTempView("silver_stores")
silver_products_df.createOrReplaceTempView("silver_products")
silver_daily_sales_summary_df.createOrReplaceTempView("silver_daily_sales_summary")

# ------------------------------------------------------------------
# 3) Transformations using Spark SQL
# ------------------------------------------------------------------

# gold.gold_sales
gold_sales_df = spark.sql("""
SELECT
  CAST(sst.transaction_id AS STRING) AS transaction_id,
  CAST(sst.store_id AS STRING) AS store_id,
  CAST(sst.product_id AS STRING) AS product_id,
  CAST(sst.sale_date AS DATE) AS sale_date,
  CAST(sst.quantity_sold AS INT) AS quantity_sold,
  CAST(sst.total_revenue AS DOUBLE) AS total_revenue
FROM silver_sales_transactions sst
""")

# gold.gold_store_performance
gold_store_performance_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.location AS STRING) AS location,
  CAST(SUM(CAST(sst.total_revenue AS DOUBLE)) AS DOUBLE) AS total_sales,
  CAST(COUNT(DISTINCT CAST(sst.transaction_id AS STRING)) AS BIGINT) AS transaction_count
FROM silver_stores ss
INNER JOIN silver_sales_transactions sst
  ON ss.store_id = sst.store_id
GROUP BY
  ss.store_id,
  ss.store_name,
  ss.location
""")

# gold.gold_product_performance
gold_product_performance_df = spark.sql("""
SELECT
  CAST(sp.product_id AS STRING) AS product_id,
  CAST(sp.product_name AS STRING) AS product_name,
  CAST(sp.category AS STRING) AS category,
  CAST(SUM(CAST(sst.total_revenue AS DOUBLE)) AS DOUBLE) AS total_sales,
  CAST(SUM(CAST(sst.quantity_sold AS INT)) AS INT) AS units_sold
FROM silver_products sp
INNER JOIN silver_sales_transactions sst
  ON sp.product_id = sst.product_id
GROUP BY
  sp.product_id,
  sp.product_name,
  sp.category
""")

# gold.gold_master_product
gold_master_product_df = spark.sql("""
SELECT
  CAST(sp.product_id AS STRING) AS product_id,
  CAST(sp.product_name AS STRING) AS product_name,
  CAST(sp.category AS STRING) AS category,
  CAST(sp.price AS FLOAT) AS price
FROM silver_products sp
""")

# gold.gold_master_store
gold_master_store_df = spark.sql("""
SELECT
  CAST(ss.store_id AS STRING) AS store_id,
  CAST(ss.store_name AS STRING) AS store_name,
  CAST(ss.location AS STRING) AS location
FROM silver_stores ss
""")

# gold.gold_aggregated_sales
gold_aggregated_sales_df = spark.sql("""
SELECT
  CAST(sdss.date AS DATE) AS date,
  CAST(sdss.total_revenue AS DOUBLE) AS total_revenue,
  CAST(sdss.total_transactions AS BIGINT) AS total_transactions,
  CAST(sdss.average_ticket_size AS DOUBLE) AS average_ticket_size
FROM silver_daily_sales_summary sdss
""")

# ------------------------------------------------------------------
# 4) Save outputs (single CSV file directly under TARGET_PATH)
# ------------------------------------------------------------------

(
    gold_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales.csv")
)

(
    gold_store_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_performance.csv")
)

(
    gold_product_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_performance.csv")
)

(
    gold_master_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_master_product.csv")
)

(
    gold_master_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_master_store.csv")
)

(
    gold_aggregated_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_aggregated_sales.csv")
)

job.commit()
```