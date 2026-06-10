```python
import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


SOURCE_PATH = "s3://sdlc-agent-bucket/engineering-agent/silver/"
TARGET_PATH = "s3://sdlc-agent-bucket/engineering-agent/gold/"
FILE_FORMAT = "csv"

# -------------------------
# 1) Read Source Tables
# -------------------------
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

# -------------------------
# 2) Create Temp Views
# -------------------------
sales_transactions_silver_df.createOrReplaceTempView("sales_transactions_silver")
products_silver_df.createOrReplaceTempView("products_silver")
stores_silver_df.createOrReplaceTempView("stores_silver")

# =========================================================
# Target: gold_sales_transaction_enriched
# =========================================================
gold_sales_transaction_enriched_sql = """
SELECT
  CAST(sts.transaction_id AS STRING)                                   AS sales_txn_id,
  CAST(sts.transaction_time AS TIMESTAMP)                              AS sales_txn_ts,
  CAST(CAST(sts.transaction_time AS TIMESTAMP) AS DATE)                AS sales_date,
  CAST(sts.store_id AS STRING)                                         AS store_id,
  CAST(ss.store_name AS STRING)                                        AS store_name,
  CAST(ss.city AS STRING)                                              AS store_city,
  CAST(ss.state AS STRING)                                             AS store_state,
  CAST(sts.product_id AS STRING)                                       AS product_id,
  CAST(ps.product_name AS STRING)                                      AS product_name,
  CAST(ps.category AS STRING)                                          AS product_category,
  CAST(ps.price AS DOUBLE)                                             AS unit_price,
  CAST(sts.quantity AS INT)                                            AS quantity,
  CAST(sts.sale_amount AS DOUBLE)                                      AS gross_sales_amount
FROM sales_transactions_silver sts
LEFT JOIN products_silver ps
  ON sts.product_id = ps.product_id
LEFT JOIN stores_silver ss
  ON sts.store_id = ss.store_id
"""

gold_sales_transaction_enriched_df = spark.sql(gold_sales_transaction_enriched_sql)

(
    gold_sales_transaction_enriched_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_sales_transaction_enriched.csv")
)

# =========================================================
# Target: gold_store_daily_sales
# =========================================================
gold_store_daily_sales_sql = """
SELECT
  CAST(CAST(sts.transaction_time AS TIMESTAMP) AS DATE)                AS sales_date,
  CAST(sts.store_id AS STRING)                                         AS store_id,
  CAST(ss.store_name AS STRING)                                        AS store_name,
  CAST(ss.city AS STRING)                                              AS store_city,
  CAST(ss.state AS STRING)                                             AS store_state,
  CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE)                 AS total_revenue,
  CAST(COUNT(sts.transaction_id) AS BIGINT)                            AS transaction_count,
  CAST(SUM(CAST(sts.quantity AS BIGINT)) AS BIGINT)                    AS total_quantity_sold
FROM sales_transactions_silver sts
INNER JOIN stores_silver ss
  ON sts.store_id = ss.store_id
GROUP BY
  CAST(CAST(sts.transaction_time AS TIMESTAMP) AS DATE),
  CAST(sts.store_id AS STRING),
  CAST(ss.store_name AS STRING),
  CAST(ss.city AS STRING),
  CAST(ss.state AS STRING)
"""

gold_store_daily_sales_df = spark.sql(gold_store_daily_sales_sql)

(
    gold_store_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_store_daily_sales.csv")
)

# =========================================================
# Target: gold_product_daily_sales
# =========================================================
gold_product_daily_sales_sql = """
SELECT
  CAST(CAST(sts.transaction_time AS TIMESTAMP) AS DATE)                AS sales_date,
  CAST(sts.product_id AS STRING)                                       AS product_id,
  CAST(ps.product_name AS STRING)                                      AS product_name,
  CAST(ps.category AS STRING)                                          AS product_category,
  CAST(SUM(CAST(sts.sale_amount AS DOUBLE)) AS DOUBLE)                 AS total_revenue,
  CAST(COUNT(sts.transaction_id) AS BIGINT)                            AS transaction_count,
  CAST(SUM(CAST(sts.quantity AS BIGINT)) AS BIGINT)                    AS total_quantity_sold
FROM sales_transactions_silver sts
INNER JOIN products_silver ps
  ON sts.product_id = ps.product_id
GROUP BY
  CAST(CAST(sts.transaction_time AS TIMESTAMP) AS DATE),
  CAST(sts.product_id AS STRING),
  CAST(ps.product_name AS STRING),
  CAST(ps.category AS STRING)
"""

gold_product_daily_sales_df = spark.sql(gold_product_daily_sales_sql)

(
    gold_product_daily_sales_df.coalesce(1)
    .write.mode("overwrite")
    .format("csv")
    .option("header", "true")
    .save(f"{TARGET_PATH}/gold_product_daily_sales.csv")
)

job.commit()
```