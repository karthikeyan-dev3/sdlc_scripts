import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")

SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------------------------------------------------
# Read Source Tables (Silver)
# -------------------------------------------------------------------
dim_store_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_store_silver.{FILE_FORMAT}/")
)
dim_product_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/dim_product_silver.{FILE_FORMAT}/")
)
sales_txn_silver_df = (
    spark.read.format(FILE_FORMAT)
    .option("header", "true")
    .load(f"{SOURCE_PATH}/sales_txn_silver.{FILE_FORMAT}/")
)

dim_store_silver_df.createOrReplaceTempView("dim_store_silver")
dim_product_silver_df.createOrReplaceTempView("dim_product_silver")
sales_txn_silver_df.createOrReplaceTempView("sales_txn_silver")

# -------------------------------------------------------------------
# Target: gold_dim_store
# Source: silver.dim_store_silver
# -------------------------------------------------------------------
gold_dim_store_df = spark.sql(
    """
SELECT
  CAST(dss.store_id AS STRING) AS store_id,
  CAST(dss.store_name AS STRING) AS store_name,
  CAST(dss.state AS STRING) AS store_state,
  CAST(dss.city AS STRING) AS store_city,
  CAST(
    CASE
      WHEN dss.state IN ('CT','ME','MA','NH','RI','VT','NJ','NY','PA') THEN 'NORTHEAST'
      WHEN dss.state IN ('IL','IN','MI','OH','WI','IA','KS','MN','MO','NE','ND','SD') THEN 'MIDWEST'
      WHEN dss.state IN ('DE','FL','GA','MD','NC','SC','VA','DC','WV','AL','KY','MS','TN','AR','LA','OK','TX') THEN 'SOUTH'
      WHEN dss.state IN ('AZ','CO','ID','MT','NV','NM','UT','WY','AK','CA','HI','OR','WA') THEN 'WEST'
      ELSE 'UNKNOWN'
    END AS STRING
  ) AS store_region
FROM dim_store_silver dss
"""
)

(
    gold_dim_store_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_store.csv")
)

gold_dim_store_df.createOrReplaceTempView("gold_dim_store")

# -------------------------------------------------------------------
# Target: gold_dim_product
# Source: silver.dim_product_silver
# -------------------------------------------------------------------
gold_dim_product_df = spark.sql(
    """
SELECT
  CAST(dps.product_id AS STRING) AS product_id,
  CAST(dps.product_name AS STRING) AS product_name,
  CAST(dps.category AS STRING) AS category_name,
  CAST(dps.category AS STRING) AS category_id,
  CAST(dps.brand AS STRING) AS brand_name
FROM dim_product_silver dps
"""
)

(
    gold_dim_product_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_dim_product.csv")
)

gold_dim_product_df.createOrReplaceTempView("gold_dim_product")

# -------------------------------------------------------------------
# Target: gold_sales_txn_enriched
# Source: silver.sales_txn_silver LEFT JOIN silver.dim_store_silver LEFT JOIN silver.dim_product_silver
# -------------------------------------------------------------------
gold_sales_txn_enriched_df = spark.sql(
    """
SELECT
  CAST(sts.transaction_id AS STRING) AS sales_txn_id,
  CAST(sts.transaction_time AS TIMESTAMP) AS sales_txn_ts,
  CAST(sts.transaction_time AS DATE) AS sales_date,
  CAST(sts.store_id AS STRING) AS store_id,
  CAST(sts.product_id AS STRING) AS product_id,
  CAST(sts.quantity AS INT) AS quantity,
  CAST(sts.sale_amount AS DOUBLE) AS net_sales_amount,
  CAST(sts.sale_amount AS DOUBLE) AS gross_sales_amount,
  CAST(sts.sale_amount - sts.sale_amount AS DOUBLE) AS discount_amount,
  CAST(sts.sale_amount - sts.sale_amount AS DOUBLE) AS tax_amount,
  CAST(
    CASE
      WHEN sts.transaction_id IS NOT NULL THEN 'USD'
      ELSE 'USD'
    END AS STRING
  ) AS currency_code
FROM sales_txn_silver sts
LEFT JOIN dim_store_silver dss
  ON sts.store_id = dss.store_id
LEFT JOIN dim_product_silver dps
  ON sts.product_id = dps.product_id
"""
)

(
    gold_sales_txn_enriched_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_txn_enriched.csv")
)

gold_sales_txn_enriched_df.createOrReplaceTempView("gold_sales_txn_enriched")

# -------------------------------------------------------------------
# Target: gold_store_daily_performance
# Source: gold.gold_sales_txn_enriched
# -------------------------------------------------------------------
gold_store_daily_performance_df = spark.sql(
    """
SELECT
  CAST(gste.sales_date AS DATE) AS sales_date,
  CAST(gste.store_id AS STRING) AS store_id,
  CAST(SUM(gste.net_sales_amount) AS DOUBLE) AS revenue_amount,
  CAST(COUNT(DISTINCT gste.sales_txn_id) AS BIGINT) AS transaction_count,
  CAST(SUM(gste.quantity) AS BIGINT) AS quantity_sold
FROM gold_sales_txn_enriched gste
GROUP BY
  gste.sales_date,
  gste.store_id
"""
)

(
    gold_store_daily_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_daily_performance.csv")
)

gold_store_daily_performance_df.createOrReplaceTempView("gold_store_daily_performance")

# -------------------------------------------------------------------
# Target: gold_product_daily_performance
# Source: gold.gold_sales_txn_enriched LEFT JOIN silver.dim_product_silver
# -------------------------------------------------------------------
gold_product_daily_performance_df = spark.sql(
    """
SELECT
  CAST(gste.sales_date AS DATE) AS sales_date,
  CAST(gste.product_id AS STRING) AS product_id,
  CAST(dps.category AS STRING) AS category_id,
  CAST(SUM(gste.net_sales_amount) AS DOUBLE) AS revenue_amount,
  CAST(COUNT(DISTINCT gste.sales_txn_id) AS BIGINT) AS transaction_count,
  CAST(SUM(gste.quantity) AS BIGINT) AS quantity_sold
FROM gold_sales_txn_enriched gste
LEFT JOIN dim_product_silver dps
  ON gste.product_id = dps.product_id
GROUP BY
  gste.sales_date,
  gste.product_id,
  dps.category
"""
)

(
    gold_product_daily_performance_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_daily_performance.csv")
)

gold_product_daily_performance_df.createOrReplaceTempView("gold_product_daily_performance")
